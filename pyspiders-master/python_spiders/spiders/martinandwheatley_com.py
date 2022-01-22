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
    name = 'martinandwheatley_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.martinandwheatley.com/properties/lettings/tag-flat",
                    "https://www.martinandwheatley.com/properties/lettings/tag-apartment"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.martinandwheatley.com/properties/lettings/tag-house",
                    "https://www.martinandwheatley.com/properties/lettings/tag-bungalows",
                    "https://www.martinandwheatley.com/properties/lettings/tag-maisonette",
                    "https://www.martinandwheatley.com/properties/lettings/tag-new-home",
                    "https://www.martinandwheatley.com/properties/lettings/tag-detached",
                    "https://www.martinandwheatley.com/properties/lettings/tag-semi-detached",
                    "https://www.martinandwheatley.com/properties/lettings/tag-cottage"
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
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='propList box']//div[@class='row-fluid']"):
            url = item.xpath(".//a/@href").get()
            status = item.xpath(".//img[contains(@src,'letagreed')]/@src").get()
            if not status:
                yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:            
            p_url = response.url.split("/page-")[0] + f"/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Martinandwheatley_PySpider_united_kingdom")

        external_id = response.xpath("//li[contains(@class,'propertyRef')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'propHeading')]//h2/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//div[contains(@class,'propHeading')]//h2/text()").getall())
        if address:
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'propHeading')]//h2//span//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'propertyDetails')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bed')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'propertyDetailPhotos')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//a[contains(@data-fancybox-group,'propertyplans')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@id,'propertyDetails')]//p[contains(.,'available on')]/text()").getall())
        if available_date:
            available_date = available_date.strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@id,'propertyDetails')]//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'Garage') or contains(.,'garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//div[contains(@id,'propertyDetails')]//li[contains(.,'terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//div[contains(@id,'propertyDetails')]//li[contains(.,'floor')]//text()[not(contains(.,'flooring') or contains(.,'floors'))]").get()
        if floor:
            floor = floor.lower().replace("modern","").split("floor")[0].strip()
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()  
            if not(latitude == "0.0" or longitude == "0.0"):
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "MARTIN AND WHEATLEY")
        item_loader.add_value("landlord_phone", "01932 855801")

        yield item_loader.load_item()