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
    name = 'piemonteaffitti_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Piemonteaffitti_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=9&price_min=200&price_max=1200&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=10&price_min=30&price_max=1100&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=10&price_min=30&price_max=1100&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=12&price_min=550&price_max=1400&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=13&price_min=600&price_max=1600&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=3&price_min=150&price_max=650&square_min=0&square_max=3025&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=18&price_min=200&price_max=700&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=17&price_min=300&price_max=900&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=2&price_min=449&price_max=1000&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=4&price_min=680&price_max=1500&land_square=&do-term-search=0&sApId=&ot=",
                ],      
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=22&price_min=400&price_max=1200&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=23&price_min=200&price_max=470&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot="
                ],
                "property_type" : "house" 
            },
            {
                "url" : [
                    "https://www.piemonteaffitti.it/search?city%5B%5D=0&apType=1&objType=1&price_min=55&price_max=700&square_min=0&square_max=3025&room_min=0&room_max=10&floor_min=0&floor_max=30&do-term-search=0&sApId=&ot=",
                ],
                "property_type" : "room" 
            }
          
           
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='appartment_box']/div"):
            lat = item.xpath("./@data-lat").get()
            lng = item.xpath("./@data-lng").get()
            data_id = item.xpath("./@data-ap-id").get()

            follow_url = item.xpath(".//div[@class='apartment-title']/a/@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"lat":lat,"lng":lng,"data_id":data_id})

        pagination = response.xpath("//li[@class='next']/a/@href").get()
        if pagination:
            follow_url = response.urljoin(pagination)
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        studio_check = response.xpath("//dd/text()[contains(.,'Monolocale')]").get()
        if studio_check:
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.meta.get('data_id'))
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))

        item_loader.add_xpath("title", "//div[@class='div-title']/h1/text()")
     
        address = response.xpath("//dt[contains(.,'Indirizzo:')]//following-sibling::dd//text()").get()
        if address:
            item_loader.add_value("address",address)
          
        city = response.xpath("//dt[contains(.,'Indirizzo:')]//following-sibling::dd//text()").get()
        if city:
            city=city.strip().split(",")[:1]
            item_loader.add_value("city",city)

        desc = "".join(response.xpath("//dt[contains(.,'Descrizione:')]//following-sibling::dd//text()").getall())
        desc = desc.replace("\u00e8","").replace("\u20ac","").replace("\u201d","").replace("\u2019","").replace("\u2013","").replace("\u00c0","").replace("\u00e0","").replace("\r","").replace("\n","").replace("\t","")
        if desc:
            item_loader.add_value("description", desc)

        price = response.xpath("//dt[contains(.,'Prezzo richiesto:')]//following-sibling::dd//following-sibling::span//text()").get()
        if price:
            price=price.split("€")[:1]
            item_loader.add_value("rent",price)
        item_loader.add_value("currency", "EUR")
        
        total_room = 0
        bathroom_count = response.xpath("//dt[contains(.,'Descrizione')]/following-sibling::dd/p/text()").get()
        if bathroom_count and "bagni" in bathroom_count.lower():
            bathroom_count = bathroom_count.split('bagni')[0].split(',')[-1].strip()
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
                total_room = bathroom_count
            else:
                bathroom_count = response.xpath("//h1[@class='h1-ap-title']/text()[contains(.,'bagni')]").get()
                if bathroom_count:
                    bathroom_count = bathroom_count.split('bagni')[0].split(',')[-1].split('con')[-1].strip()
                    if bathroom_count.isdigit():
                        item_loader.add_value("bathroom_count", bathroom_count)
                        total_room = int(bathroom_count)
        elif bathroom_count and "bagno" in bathroom_count.lower():
            bathroom_count = 1
            item_loader.add_value("bathroom_count", bathroom_count)
            total_room = bathroom_count
        
        if property_type == "studio":
            total_room = 1
            item_loader.add_value("room_count", total_room)
        else:
            room = response.xpath("//dt[contains(.,'Descrizione')]/following-sibling::dd/p/text()").get()
            if room:
                room = room.split('camere')[0].split(',')[-1].strip()
                if room.isdigit():
                    total_room = int(total_room) + int(room)
                    item_loader.add_value("room_count", total_room)
                else:
                    room2 = response.xpath("//div[@class='viewapartment-description-top']/div/strong/text()").get()
                    if room2:
                        room2 = room2.split('camere')[0].split(',')[-1].split('con')[-1].strip()
                        if room2.isdigit():
                            room = room2
                            total_room = int(total_room) + int(room)
                            item_loader.add_value("room_count", total_room)

        square = response.xpath("//dt[contains(.,'Superficie totale:')]//following-sibling::dd//text()").get()
        if square:
            square=square.split(" ")[:1]
            item_loader.add_value("square_meters", square)

        terrace = response.xpath("//ul[contains(@class,'apartment-description-ul list-unstyled')]//a[contains(.,'Terrazzo')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//ul[contains(@class,'apartment-description-ul list-unstyled')]//a[contains(.,'Finestre')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_phone", "011 - 4332424 - 011 - 4506453 ")
        item_loader.add_value("landlord_name", "Piemonte Affitti")


        yield item_loader.load_item()