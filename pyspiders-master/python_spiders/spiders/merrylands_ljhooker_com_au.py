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
    name = 'merrylands_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://merrylands.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://merrylands.ljhooker.com.au/search/house-for-rent/page-1?surrounding=True&liveability=False",
                    "https://merrylands.ljhooker.com.au/search/townhouse-for-rent/page-1?surrounding=True&liveability=False",
                    "https://merrylands.ljhooker.com.au/search/duplex_semi_detached-for-rent/page-1?surrounding=True&liveability=False",
                    "https://merrylands.ljhooker.com.au/search/penthouse-for-rent/page-1?surrounding=True&liveability=False",
                    "https://merrylands.ljhooker.com.au/search/terrace-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://merrylands.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type": "studio"
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
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='property-details']/a[@class='track-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Merrylands_ljhooker_PySpider_australia")

        external_id = response.xpath("//div[contains(@class,'code')]//text()").get()
        if external_id:
            external_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//nav[contains(@class,'property-nav')]//following-sibling::h2//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.split(",")[0].strip().split(" ")[-1]
            item_loader.add_value("address", address)

        city_zipcode = response.xpath("//script[contains(.,'addressLocality')]//text()").get()
        if city_zipcode:
            city = city_zipcode.split('addressLocality":"')[1].split('"')[0]
            zipcode = city_zipcode.split('postalCode":"')[1].split('"')[0]
            region =city_zipcode.split('addressRegion":"')[1].split('"')[0] 
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", "{} {}".format(region,zipcode))

        rent = response.xpath("//div[contains(@class,'property-heading')]//h2//text()").get()
        if rent:
            rent = rent.split("$")[1].strip().split(" ")[0].replace(",","")
            rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'property-text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'slidethumb')]//div[contains(@class,'thumb')]//@data-cycle-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Date Available')]/text()").get()
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'property-text')]//text()[contains(.,'parking') or contains(.,'garage') or contains(.,'Garage') or contains(.,'car space')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//span[@class='carpot']/text()").extract_first()
            if parking:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//div[contains(@class,'property-text')]//text()[contains(.,'Balcon') or contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        floor = response.xpath("//div[contains(@class,'property-text')]//text()[contains(.,'Floor') or contains(.,'floor ')][not(contains(.,'Flooring'))]").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.strip())
            
        dishwasher = response.xpath("//div[contains(@class,'property-text')]//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'GeoCoordinates')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//script[contains(.,'telephone')]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.split('telephone":"')[0].split('name":"')[-1].split('"')[0]
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//script[contains(.,'email')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.split('email":"')[1].split('"')[0]
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//script[contains(.,'telephone')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('telephone":"')[1].split('"')[0]
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()