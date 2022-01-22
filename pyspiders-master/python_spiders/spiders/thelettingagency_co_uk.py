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
    name = 'thelettingagency_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Thelettingagency_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://thelettingagency.co.uk/properties/rental-list?mefibs-form-filters-field_price_value=&mefibs-form-filters-field_price_value_1=&mefibs-form-filters-field_bedrooms_value=All&mefibs-form-filters-field_bedrooms_value_1=All&mefibs-form-filters-field_property_type_value=9&mefibs-form-filters-sort_by=field_price_value&mefibs-form-filters-sort_order=DESC&mefibs-form-filters-items_per_page=10&mefibs-form-filters-mefibs_block_id=filters",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://thelettingagency.co.uk/properties/rental-list?mefibs-form-filters-field_price_value=&mefibs-form-filters-field_price_value_1=&mefibs-form-filters-field_bedrooms_value=All&mefibs-form-filters-field_bedrooms_value_1=All&mefibs-form-filters-field_property_type_value=10&mefibs-form-filters-sort_by=field_price_value&mefibs-form-filters-sort_order=DESC&mefibs-form-filters-items_per_page=10&mefibs-form-filters-mefibs_block_id=filters",
                    "https://thelettingagency.co.uk/properties/rental-list?mefibs-form-filters-field_price_value=&mefibs-form-filters-field_price_value_1=&mefibs-form-filters-field_bedrooms_value=All&mefibs-form-filters-field_bedrooms_value_1=All&mefibs-form-filters-field_property_type_value=8&mefibs-form-filters-sort_by=field_price_value&mefibs-form-filters-sort_order=DESC&mefibs-form-filters-items_per_page=10&mefibs-form-filters-mefibs_block_id=filters",
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
        for item in response.xpath("//div[@class='property-list-item']/div/div"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent = response.xpath("//h1/text()").get()
        if rent and "£" in rent:
            rent = rent.split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//div[@id='full-description']//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        else:
            description = " ".join(response.xpath("//p[contains(@class,'description')]//text()").getall())
            if description:
                item_loader.add_value("description", description.strip())
            
        address = response.xpath("//div[@class='intro']/p/text()").get()
        city = ""
        if address and address.strip():
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
        else:
            address = description.split(" in ")[1].split(" is ")[0].strip()
            item_loader.add_value("address", address.strip())
            city = address.strip()
        
        if city and " " not in city:
            item_loader.add_value("city", city)
            
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            if latitude:
                item_loader.add_value("latitude", latitude)
            if longitude:
                item_loader.add_value("longitude", longitude)
        
        import dateparser
        available_date = response.xpath("//ul[@class='features']/li//text()").get()
        if available_date:
            available_date = available_date.split("Available")[-1].replace(".","").strip()
            if "now" not in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        room_count = response.xpath("//p/strong[contains(.,'ROOM') or contains(.,'Bedroom')]/following-sibling::strong/text()").getall()
        rooms = []
        for room in room_count:
            if "\\" not in room and "x" not in room:
                rooms.append(room)
        from word2number import w2n
        if rooms:
            if rooms[-1].isdigit():
                item_loader.add_value("room_count", rooms[-1])
            else:
                item_loader.add_value("room_count",w2n.word_to_num(rooms[-1]))
        elif "one bedroom" in description:
            item_loader.add_value("room_count", "1")
        parking = "".join(response.xpath("//p/strong[contains(.,'Parking')]/parent::p/text()").getall())
        if parking and "Allocated" in parking:
            item_loader.add_value("parking", True)
            
        item_loader.add_value("landlord_name", "The Letting Agency")
        item_loader.add_value("landlord_phone", "01480 570 111")
        item_loader.add_value("landlord_email", "stives@thelettingagency.co.uk")
        
        yield item_loader.load_item()