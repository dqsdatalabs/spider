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
    name = 'professionalsmildura_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://professionalsmildura.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=house&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                    "https://professionalsmildura.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=townhouse&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                ],
                "property_type": "house"
            },
	        {
                "url": [
                    "https://professionalsmildura.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&localities=&postal_code=&status=rent&suburb-box=&property_types=unit&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_carparks=&min_land_size=&land_size_units=m2",
                ],
                "property_type": "apartment"
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
        for item in response.xpath("//div[contains(@class,'details-wrapper')]//parent::a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Professionalsmildura_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        address = response.xpath("//div[contains(@class,'address')]//text()").get()
        if address:
            city = address.split("VIC")[0].split(",")[1].strip()
            zipcode = address.split(city)[1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'price')]//text()").get()
        if rent:
            if "pw" in rent.lower():                
                rent = rent.strip().replace("$","").replace(",","").replace("pw","")
                rent = int(rent)*4
            else:
                rent = rent.strip().replace("$","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//div[contains(@id,'description')]//text()[contains(.,'Bond')]").get()
        if deposit:
            deposit = deposit.split("$")[1].replace(",","").strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@id,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//img[contains(@class,'bed')]//parent::div//text()").getall())
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//img[contains(@class,'bath')]//parent::div//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery-container')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[contains(@id,'description')]//text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = "".join(response.xpath("//img[contains(@class,'car')]//parent::div//text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        latitude_longitude = response.xpath("//div[contains(@class,'property-map')]//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center=')[1].split(',')[0]
            longitude = latitude_longitude.split('center=')[1].split(',')[1].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "PROFESSIONALS REAL ESTATE MILDURA")
        item_loader.add_value("landlord_phone", "03 5021 1900")

        yield item_loader.load_item()