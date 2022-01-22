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
    name = 'agence_antibes_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    post_url = "http://www.agence-antibes.com/en/search/"
    current_index = 0
    other_prop = ["2"]
    other_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type[]": "1",
            "price": "",
            "age": "",
            "tenant_min": "",
            "tenant_max": "",
            "rent_type": "",
            "newprogram_delivery_at": "",
            "newprogram_delivery_at_display": "",
            "currency": "EUR",
            "customroute": "",
            "homepage": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//div[@class='buttons']//a[@class='button']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//li[@class='nextpage']/a/@href").get():    
            p_url = f"http://www.agence-antibes.com/en/search/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type[]": self.other_prop[self.current_index],
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
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
        item_loader.add_value("external_source", "Agence_Antibes_PySpider_france")
        external_id = response.xpath("//li[contains(.,'Ref')]//text()").get()
        if external_id:
            external_id = external_id.split("Ref.")[1].strip()
            item_loader.add_value("external_id", external_id)     
        
        title = " ".join(response.xpath("//span[contains(@class,'selectionLink')]//following-sibling::h2//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            address = title.replace("Studio","").replace("Apartment","")
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        square_meters = response.xpath("//li[contains(.,'Surface')]/span//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(",")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[contains(@class,'selectionLink')]//following-sibling::ul//li[contains(.,'€')]//text()").get()
        if rent:
            rent = rent.strip().split("€")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Guarantee')]//span//text()").get()
        if deposit:
            deposit = deposit.strip().replace("€","").replace(",","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Fees')]//span//text()").get()
        if utilities:
            utilities = utilities.strip().split("€")[0]
            item_loader.add_value("utilities", utilities)

        room_count = response.xpath("//li[contains(.,'Room')]//span//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'show-carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//li[contains(.,'Terrace')]//span//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//img[contains(@alt,'Energy - Conventional consumption')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1]
            if energy_label > "0":
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//li[contains(.,'Floor')]//span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('_map_2 =')[1].split("L.marker([")[1].split(',')[0]
            longitude = latitude_longitude.split('_map_2 =')[1].split('L.marker([')[1].split(",")[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "AGENCE DU VIEIL ANTIBES")
        item_loader.add_value("landlord_phone", "+33 4 93 34 08 04")

        status = response.xpath("//li[contains(.,'Availability')]//span//text()[contains(.,'Rented')]").get()
        if status:
            return
        else:
            yield item_loader.load_item()