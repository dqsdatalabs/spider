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
    name = 'rhburwood_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                "https://www.raineandhorne.com.au/burwood/search/properties?listing_type=residential&status=active&offer_type_code=rental&refined=&property_type=Apartment&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_car_spaces=&min_land_area_sqm=&surrounding_suburbs=0",
                "https://www.raineandhorne.com.au/burwood/search/properties?listing_type=residential&status=active&offer_type_code=rental&refined=&property_type=Unit&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_car_spaces=&min_land_area_sqm=&surrounding_suburbs=0,"
                ],
                "property_type": "apartment",
            },
	        {
                "url":[
                    "https://www.raineandhorne.com.au/burwood/search/properties?listing_type=residential&status=active&offer_type_code=rental&refined=&property_type=House&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_car_spaces=&min_land_area_sqm=&surrounding_suburbs=0",
                    "https://www.raineandhorne.com.au/burwood/search/properties?listing_type=residential&status=active&offer_type_code=rental&refined=&property_type=Townhouse&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_car_spaces=&min_land_area_sqm=&surrounding_suburbs=0",
                ],
                "property_type": "house",
            },
            {
                "url":[
                    "https://www.raineandhorne.com.au/burwood/search/properties?listing_type=residential&status=active&offer_type_code=rental&refined=&property_type=Studio&min_price=&max_price=&min_bedrooms=&min_bathrooms=&min_car_spaces=&min_land_area_sqm=&surrounding_suburbs=0",
                ],
                "property_type": "studio",
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
        for item in response.xpath("//div[contains(@class,'item property_item')]/a/@href[not(contains(.,'login'))]").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "login" not in response.url:
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", "Rhburwood_Com_PySpider_australia")
            
            item_loader.add_css("title", "title")
            
            address = "".join(response.xpath("//span[@itemprop='address']/span/text()").getall())
            if address:
                item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
            
            city = response.xpath("//span[@itemprop='addressLocality']/text()").get()
            if city:
                item_loader.add_value("city", city.strip().strip(","))
            
            zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
            if zipcode:
                item_loader.add_value("zipcode", f"NSW {zipcode.strip()}")
            
            rent = response.xpath("//h4[@class='details-desc-price']/text()").get()
            if rent:
                price = rent.strip().replace("-"," ").split("$")[1].split(" ")[0]
                item_loader.add_value("rent", int(price)*4)
            item_loader.add_value("currency", "AUD")
            
            room_count = response.xpath("//div[@class='details-icons']//li[@class='beds']/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            
            bathroom_count = response.xpath("//div[@class='details-icons']//li[@class='baths']/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
            
            description = " ".join(response.xpath("//div[contains(@class,'description-info-inner')]//p//text()").getall())
            if description:
                item_loader.add_value("description", description.strip())
            
            images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'slider__slide')]//@style[contains(.,'url')]").getall()]
            if images:
                item_loader.add_value("images", images)

            external_id = response.xpath("//span[contains(.,'ID')]/following-sibling::span/text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id.strip())
            
            parking = response.xpath("//span[contains(.,'Garage')]/following-sibling::span/text()[.!='0'] | //div[@class='details-icons']//li[@class='cars']/text()[.!='0']").get()
            if parking:
                item_loader.add_value("parking", True)
            
            latitude_longitude = response.xpath("//script[contains(.,'coordinates:')]/text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('coordinates: [')[1].split(',')[0]
                longitude = latitude_longitude.split('coordinates: [')[1].split(',')[1].split(']')[0].strip()
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
            landlord_name = response.xpath("//div[contains(@class,'info__name')]/a/text()").get()
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name)
            
            landlord_phone = response.xpath("//button/@data-phone").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
            
            yield item_loader.load_item()