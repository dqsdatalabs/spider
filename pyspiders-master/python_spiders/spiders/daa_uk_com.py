# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'daa_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://daa-uk.com/property-search/?status=for-rent&type=flat",
                    "http://daa-uk.com/property-search/?status=for-rent&type=apartment",
                    "http://daa-uk.com/property-search/?status=for-rent&type=split-level-apartment",
                    "http://daa-uk.com/property-search/?status=for-rent&type=modern-apartment",
                    "http://daa-uk.com/property-search/?status=for-rent&type=residential"

                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://daa-uk.com/property-search/?status=for-rent&type=maisonette",
                    "http://daa-uk.com/property-search/?status=for-rent&type=penthouse",
                    "http://daa-uk.com/property-search/?status=for-rent&type=house",
                    "http://daa-uk.com/property-search/?status=for-rent&type=split-level-maisonette",
                    "http://daa-uk.com/property-search/?status=for-rent&type=townhouse",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "http://daa-uk.com/property-search/?status=for-rent&type=studio-apartment",
                   ],
                "property_type": "studio"
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
        for link in response.xpath("//p/a[@class='more-details']/@href").getall(): 
            yield Request(link, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
  
        next_page = response.xpath("//div[@class='pagination']/a[@class='real-btn current']/following-sibling::a[1]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        studio_property = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Studio')]//text()").get()
        if studio_property:
            property_type = "studio"
            item_loader.add_value("property_type", property_type)
        else:
            property_type = response.meta.get('property_type')
            item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", "Daa_PySpider_united_kingdom")

        external_id = response.xpath("//h4[contains(.,'Property ID')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//h5[contains(@class,'price')]//span[contains(.,'£')]/text()").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.strip().replace("£","").replace(",","").strip().split(" ")[0]
                rent = int(rent)*4
            else:
                rent = rent.strip().replace("£","").replace(",","").strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if property_type == "studio":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//div[contains(@class,'property-meta')]//span[contains(.,'Bedroom')]/text()").get()
            if room_count:
                room_count = room_count.strip().split("\u00a0")[0]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'property-meta')]//span[contains(.,'Bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split("\u00a0")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-slider')]//li//@href").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@class,'floor-plan-content')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'Available')]//following-sibling::span//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'property-meta')]//span[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'lat\"')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lang":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "DAA RESIDENTIAL")
        item_loader.add_value("landlord_phone", "020 7702 1111")
        item_loader.add_value("landlord_email", "help@daa-uk.com")
   
        yield item_loader.load_item()