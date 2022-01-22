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
from word2number import w2n

class MySpider(Spider):
    name = 'fleetmilne_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    url = "https://fleetmilne.co.uk/flats-to-rent-in-birmingham/"

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'max-price-sale': '',
                    'max-price-rent': '',
                    'beds': '0',
                    'type': 'B',
                    'order': 'availability'

                },
                "property_type" : "apartment",
            }
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"], "formdata": item["formdata"]})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'fm-prop-det')]"):
            f_url = item.xpath(".//@href[contains(.,'property')]").get()
            room_count = item.xpath(".//span[contains(@class,'bed')]//text()").get()
            bathroom_count = item.xpath(".//span[contains(@class,'bath')]//text()").get()
            yield Request(response.urljoin(f_url), callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count":room_count,"bathroom_count":bathroom_count})
            seen = True
        
        if page==2 or seen:
            next_url =f"https://fleetmilne.co.uk/flats-to-rent-in-birmingham/page/{page}/?beds&min-price=0&max-price=900000&order=availability&neigh&type=B"
            yield Request(next_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        studio = response.xpath("//li[contains(.,'Studio') or contains(.,'studio')]//text()").get()
        if studio:
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Fleetmilne_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("-")[0])

        title = " ".join(response.xpath("//div[contains(@class,'fm-panel-inner')]//h1/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'fm-panel-inner')]//h1/text()").get()
        if address:
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//meta[@property='og:description']/@content[contains(.,' city')]").get()
        if city:
            item_loader.add_value("city", city.split(" city")[0].strip().split(" ")[-1])

        rent = response.xpath("//div[contains(@class,'fm-panel-inner')]//div[contains(@class,'price')]//text()").get()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
                rent = int(rent)*4
            else:
                rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//h2[contains(.,'Description')]//following-sibling::div//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.meta.get('room_count')
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:   
            try:   
                bathroom = response.xpath("//li[contains(.,'bathroom')]//text()").get()
                bathroom = bathroom.split("bathroom").strip().split(" ")[-1]
                bathroom_count = w2n.word_to_num(bathroom)
                item_loader.add_value("bathroom_count", bathroom_count)

            except:
                pass
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@data-content,'photos')]//@style[contains(.,'background-image')]").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//img[contains(@src,'Floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'fm-panel-inner')]//div[contains(@class,'status')]//text()").getall())
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'fm-features')]//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'garage') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'fm-features')]//li[contains(.,'balcon') or contains(.,'balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[contains(@class,'fm-features')]//li[contains(.,'Furnished') or contains(.,'furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//div[contains(@class,'fm-features')]//li[contains(.,'floor') or contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip()
            if " " in floor:
                floor = floor.split(" ")[-1]
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'center = { lat:')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center = { lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('center = { lat:')[1].split("lng:")[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "FleetMilne")
        item_loader.add_value("landlord_phone", "0121 366 0456 ")
        item_loader.add_value("landlord_email", "property@fleetmilne.co.uk")
        
        yield item_loader.load_item()