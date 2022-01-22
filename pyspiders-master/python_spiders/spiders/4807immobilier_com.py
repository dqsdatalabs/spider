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
    name = '4807immobilier_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    post_url = "https://www.4807immobilier.com/recherche/annonce?market=rent"
    current_index = 0
    other_prop = ["90"]
    other_type = ["apartment"]
    def start_requests(self):
        yield Request(
            self.post_url,
            callback=self.jump,
        )
    def jump(self, response):
        token = response.xpath("//input[@id='rent_property_search__token']/@value").get()
        formdata = {
            "rent_property_search[market]": "2",
            "rent_property_search[category]": "13",
            "rent_property_search[budgetMax]": "",
            "rent_property_search[catchmentRadius]": "15",
            "rent_property_search[sizeMin]": "",
            "rent_property_search[sizeMax]": "",
            "rent_property_search[bedroomsMin]": "",
            "rent_property_search[furnished]": "",
            "rent_property_search[free]": "",
            "rent_property_search[submit]": "",
            "rent_property_search[_token]": token,
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "house"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//a[@class='block-link']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//a[@rel='next']/@href").get():
            p_url = f"https://www.4807immobilier.com/recherche/annonce/page-{page}/prox-page?market=rent&listId=properties"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            token = response.xpath("//input[@id='rent_property_search__token']/@value").get()
            formdata = {
                "rent_property_search[market]": "2",
                "rent_property_search[category]": self.other_prop[self.current_index],
                "rent_property_search[budgetMax]": "",
                "rent_property_search[catchmentRadius]": "15",
                "rent_property_search[sizeMin]": "",
                "rent_property_search[sizeMax]": "",
                "rent_property_search[bedroomsMin]": "",
                "rent_property_search[furnished]": "",
                "rent_property_search[free]": "",
                "rent_property_search[submit]": "",
                "rent_property_search[_token]": token,
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "4807immobilier_PySpider_france")
        external_id = response.xpath("//p[@class='mandate']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
    
        city ="".join(response.xpath("//div[@class='localisation']/a[i[@class='icon-map-pin']]/text()").getall())
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())
        floor = response.xpath("//li[contains(.,'Étage :')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
        room_count = response.xpath("//li[@class='bedrooms']//text()[normalize-space()]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("c")[0])
        else:
            room_count = response.xpath("//li[@class='rooms']//text()[normalize-space()]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("p")[0])
        bathroom_count = response.xpath("//li[@class='bathrooms']//text()[normalize-space()]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("s")[0])
        description = "".join(response.xpath("//div[@class='wysiwyg-content']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//li[@class='size']//text()[normalize-space()]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        rent = response.xpath("//div[@class='prices']/p[@class!='monthly-price-details']//text()").re_first(r'\d+.*\d+')
        if rent:
            item_loader.add_value("rent", rent.replace(' ',''))
            item_loader.add_value("currency", "EUR")
        deposit = response.xpath("//div[@class='wysiwyg-content']/text()[contains(.,'dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("dépôt de garantie")[1].split("€")[0])
        utilities = response.xpath("//div[@class='prices']/p[@class='monthly-price-details']//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("provisions pour")[0])
     
        item_loader.add_xpath("latitude", "//div[@class='marker']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='marker']/@data-lng")
        available_date = response.xpath("//div[@class='wysiwyg-content']/text()[contains(.,'Libre le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Libre le")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [response.urljoin(x) for x in response.xpath("//ul[@id='mobile-property-pictures-list']//li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)  
        item_loader.add_value("landlord_name", "4807 Immobilier")
        landlord_phone = response.xpath("//div[@class='property-agency']//p[@class='phone']/text()[normalize-space()]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        energy_label = response.xpath("//div[@class='energy-consumption-scale']/p[contains(@class,'value-letter letter-')]/@class").get()
        if energy_label:
            energy = energy_label.split("letter-")[-1]
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy)
        
        balcony = response.xpath("//li[contains(.,'Balcon :')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//li[contains(.,'Garages :')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        yield item_loader.load_item()