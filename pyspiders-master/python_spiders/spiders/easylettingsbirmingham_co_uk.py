# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = 'easylettingsbirmingham_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            
            {
                "url": [
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=26&marketing_flag=67&location=&_bills_included=&minimum_bathrooms=",
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=26&marketing_flag=81&location=&_bills_included=",
                    
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=22&marketing_flag=67&location=&_bills_included=&minimum_bathrooms=",
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=22&marketing_flag=81&location=&_bills_included=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=18&marketing_flag=67&location=&_bills_included=&minimum_bathrooms=",
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=9&marketing_flag=67&location=&_bills_included=&minimum_bathrooms=",
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=18&marketing_flag=81&location=&_bills_included="
                    "https://easylettingsbirmingham.co.uk/property-list/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&bedrooms=&property_type=9&marketing_flag=81&location=&_bills_included=",
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[@class='item']"):
            status = item.xpath(".//div[@class='sold_text']/text()").get()
            if not status:
                follow_url = response.urljoin(item.xpath(".//a/@href").get())
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next page')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Easylettingsbirmingham_Co_PySpider_united_kingdom")

        external_id = response.xpath("//strong[contains(.,'Ref')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h2/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h2/text()").getall())
        if address:
            address = address.replace("Similar Properties","").strip()
            city = address.strip().split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//div[contains(@class,'content_holder')]/text()[contains(.,'Price')]").get()
        if rent:
            # rent = rent.strip().split("£")[1].split(".")[0]
            if "Price" in rent:
                rent = rent.split("Price")[1]
                rent=rent.split("/")[-1]
                rent=rent.split("£")[1].split(".")[0]
                item_loader.add_value("rent", rent)
               
            # else:
            #     rent = rent.split("£")[0].split(" ")[-1]
         
        # else: 
        #     rent = "".join(response.xpath("//div[contains(@class,'description_title_holder')]//h3//text()").getall())
        #     if rent:
        #         if "pw" in rent.lower():
        #             rent = rent.strip().split("£")[1].strip().split(" ")[0]
        #             rent = int(rent)*4
        #         else:
        #             rent = rent.strip().split("£")[1].strip().split(" ")[0]
        #         item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'content_holder')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::li/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bath')]//parent::li/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@class,'search-caorusel')]//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'content_holder')]/text()[contains(.,'Available From') or contains(.,'Available from') ]").getall())
        if available_date:
            available_date = available_date.strip().split(":")[-1].strip().split(" ")[0]
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garag') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Easy Lettings Birmingham Ltd")
        item_loader.add_value("landlord_phone", "0121 472 6969")
        item_loader.add_value("landlord_email", "sales@easylettingsbirmingham.co.uk")

        yield item_loader.load_item()