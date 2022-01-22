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
    name = 'arthurconias_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&category_ids=48",
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&category_ids=45",
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&category_ids=50",
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&size=24"
                ], 
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&category_ids=44",
                    "https://www.arthurconias.com.au/rent?listing_cat=rental&category_ids=46"
                ], 
                "property_type": "house"
            },
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
        for item in response.xpath("//div[contains(@class,'search-results-description-wrap')]//@href[contains(.,'property')]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='property-status-banner property-status-banner-just-listed']//text()").get()
        if status:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Arthurconias_PySpider_australia")

        external_id = response.xpath("//th[contains(.,'Property ID')]//following-sibling::td//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//th[contains(.,'Address')]//following-sibling::td//text()").get()
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//th[contains(.,'Price')]//following-sibling::td//text()").get()
        if rent:
            rent = rent.strip().replace("$","").strip().split(" ")[0]
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'property-details-section')][1]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bed')]//span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bath')]//span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slideshow-image-description')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//th[contains(.,'Available')]//following-sibling::td//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//span[contains(@class,'car')]//span//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'property-features')]//span[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[contains(@class,'property-features')]//span[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        washing_machine = response.xpath("//div[contains(@class,'property-features')]//span[contains(.,'Washing Machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_email", "rentals@arthurconias.com.au")

        landlord_name = response.xpath("//h3[contains(.,'Agent Details')]//following-sibling::div//h4//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//h3[contains(.,'Agent Details')]//following-sibling::div//i[contains(@class,'phone')]//parent::span//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
        yield item_loader.load_item()