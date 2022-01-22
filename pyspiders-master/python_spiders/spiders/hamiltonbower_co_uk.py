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
    name = 'hamiltonbower_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hamiltonbower.co.uk/advanced-search/?area=To+Rent&vp_location=&minprice=&maxprice=&minrent=&maxrent=&bedrooms=&type%5B%5D=Apartment&type%5B%5D=Flat&radius=3&vp_page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hamiltonbower.co.uk/advanced-search/?area=To+Rent&vp_location=&minprice=&maxprice=&minrent=&maxrent=&bedrooms=&type%5B%5D=House&type%5B%5D=House+-+End+Terrace&type%5B%5D=House+-+Semi-Detached&type%5B%5D=House+-+Terraced&radius=3&vp_page=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.hamiltonbower.co.uk/advanced-search/?area=To+Rent&vp_location=&minprice=&maxprice=&minrent=&maxrent=&bedrooms=&type%5B%5D=Apartment&type%5B%5D=Flat&radius=3&vp_page=1",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath(".//div[@class='property_price']/a/@href").get())
            let_agreed = item.xpath(".//div[@class='property_tagline']/text()[contains(.,'Let Agreed')]").get()
            seen = True
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = response.url.replace("&vp_page=" + str(page - 1), "&vp_page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Hamiltonbower_Co_PySpider_united_kingdom")
                
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.replace(", BD9","").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().replace("£","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'vp_content_section')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        rooms = response.xpath("//p[contains(.,'|')]//text()").get()
        if rooms:
            room_count = rooms.split("Bedrooms:")[1].split("|")[0].strip()
            bathroom_count = rooms.split("Bathrooms:")[1].strip()
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[contains(@id,'slider')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//h3[contains(.,'Floorplan')]//following-sibling::img//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'AVAILABLE')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.lower().split("available")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'PARKING')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'BALCONY')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'TERRACE')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//li[contains(.,'FLOOR')]//text()").get()
        if floor:
            floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text() ").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Hamilton Bower Estate Agent")
        item_loader.add_value("landlord_phone", "01422 20 45 45")
        item_loader.add_value("landlord_email", "sales@hamiltonbower.co.uk")

        yield item_loader.load_item()