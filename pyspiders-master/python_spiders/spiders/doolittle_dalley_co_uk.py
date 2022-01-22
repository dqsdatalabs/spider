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
    name = 'doolittle_dalley_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-apartment/status-available",
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-flat/status-available",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-maisonette/status-available",
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-semi-detached/status-available",
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-house/status-available",
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-detached/status-available",
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-bungalows/status-available",
                    
                    ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.doolittle-dalley.co.uk/search?channel=lettings&fragment=tag-studio/status-available",
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
        for item in response.xpath("//div[@class='propGrid-inner']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_list = response.xpath("//div[@id='properties_grid_toggle_view']//div[@class='pagination_footer_wrapper']/ul/li/a/@href").getall()
        if next_list:
            for i in next_list[1:-1]:
                yield Request(
                    response.urljoin(i),
                    callback=self.parse,
                    meta={"property_type": response.meta.get('property_type')}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Doolittle_Dalley_Co_PySpider_united_kingdom")

        external_id = response.xpath("//li[contains(@class,'propertyRef')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'propHeading')]//h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h2[contains(@id,'address')]//text()").get()
        if address:
            city = address.split(",")[1].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'propHeading')]//span//text()").get()
        if rent:
            rent = rent.split("£")[1].split("pcm")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'sectionContent')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bed')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        bathroom_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'image_carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//a[contains(@title,'Floor plan')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'available')]//parent::p/text()").getall())
        if available_date:
            if not "now" in available_date.lower():                
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Doolittle & Dalley ")
        item_loader.add_value("landlord_phone", "0345 3700 300")
        item_loader.add_value("landlord_email", "")
        
        yield item_loader.load_item()