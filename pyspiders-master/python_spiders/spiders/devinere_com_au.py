# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'devinere_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=Apartment&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=Terrace&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=House&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=Semi+Detached&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=Townhouse&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.devinere.com.au/rent/properties-for-lease/{}/?suburb=&list=lease&property_type=Studio&price_min=&price_max=&min_bed=&max_bed=&min_bath=&max_bath=",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='image']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base_url")
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"property_type":response.meta["property_type"], "page":page+1, "base_url":base_url}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//span[@class='price']/text()").get()
        if status and "leased" in status.lower():
            return
        rented = response.xpath("//h1[@class='title']/text()[contains(.,'Holding Deposit Received')]").get()
        if rented:            
            return
        deposittaken=response.xpath("//h1[@class='title']/text()").get()
        if deposittaken and "holding deposit received " in deposittaken.lower():
            return 
            
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Devinere_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])

        title = response.xpath("//h1//text()").get()
        item_loader.add_value("title", title)

        desc = "".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("-","")
            not_list = ["ti","mid","new","height","flow","pol","fea","qu","sm","and","be","open","with"]
            status = True
            for i in not_list:
                if i in floor.lower():
                    status = False
            if status:
                    item_loader.add_value("floor", floor.replace("-","").upper())                    

        from datetime import datetime
        import dateparser
        if "available" in desc:
            available = desc.split("available")[1].replace("from", "").replace("as of","").replace("the","").strip().split(" ")
            date = available[0]+" "+available[1]+" "+available[2]
            if "now" in date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                match = re.search(r'(\d+.\d+.\d+)', date)
                if match:
                    newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", newformat)
                else:
                    date = date.replace("on","").replace(".","")
                    date_parsed = dateparser.parse(date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("$")[1].split(" ")[0].strip().replace("-","").replace(",","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        region = response.xpath("//meta[@property='og:region']/@content").get()
        postal_code = response.xpath("//meta[@property='og:postal-code']/@content").get()
        if region and postal_code:
            item_loader.add_value("zipcode", region + " " + postal_code)

        address = "".join(response.xpath("//div[contains(@class,'address-wrap')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
        
        city = response.xpath("//div[contains(@class,'address-wrap')]//div[contains(@class,'suburb')]//text()").get()
        item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = response.xpath("//img[contains(@title,'Floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        room_count = response.xpath("//li[contains(@class,'bed')]//text()").get()
        if room_count:
            if "."in room_count:
                room_count = room_count.split(".")[0]
                item_loader.add_value("room_count", room_count)
            else:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            if "."in bathroom_count:
                bathroom_count = bathroom_count.split(".")[0]
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[contains(@class,'car')]//text()").get()
        if parking:
            item_loader.add_value("parking",True)

        balcony = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,' furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        swimming_pool = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        latitude_longitude = response.xpath("//script[contains(.,'map1')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//p[contains(@class,'name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//p[contains(@class,'contact')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_email", "rentals@devinere.com.au")

        yield item_loader.load_item()