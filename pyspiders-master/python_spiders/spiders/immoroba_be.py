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
import dateparser

class MySpider(Spider):
    name = 'immoroba_be'
    execution_type='testing'
    country='belgium'
    locale='nl' 
    external_source = "Immoroba_PySpider_belgium_nl"
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immoroba.be/te-huur?sorts=Flat&price-from=&price-to=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.immoroba.be/te-huur?sorts=Dwelling&price-from=&price-to=",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'one-third column') and ./a/div[not(contains(.,'reservatie'))]]/a[@class='img']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h3//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='adres']/text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.replace("\u00a0"," "))
            city = address.strip().split(" ")[-1]
            zipcode = address.strip().split(" ")[-2].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        external_id = response.xpath("//tr/td[contains(.,'Referentie')]/following-sibling::td//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent = response.xpath("//tr/td[contains(.,'Prij')]/following-sibling::td//text()").get()
        if rent:
            price = rent.split("/")[0].split("€")[1].strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        available_date = response.xpath(
            "//tr/td[contains(.,'Beschikbaar')]/following-sibling::td//text()[not(contains(.,'Onmiddellijk')) and not(contains(.,'In overleg'))]"
            ).get()
        if available_date:
            date_parsed = dateparser.parse(
                        available_date.strip(), date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        room_count = response.xpath("//tr/td[contains(.,'Slaapkamer')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//tr/td[contains(.,'Badkamer')]/following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//tr/td[contains(.,'Parking')]/following-sibling::td//text()").get()
        garage = response.xpath("//tr/td[contains(.,'Garage:')]/following-sibling::td//text()").get()
        if parking or garage:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//tr/td[contains(.,'Lif')]//following-sibling::td/text()").get()
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
                
        terrace = response.xpath("//tr/td[contains(.,'Terras')]/following-sibling::td//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        energy = response.xpath("//tr/td[contains(.,'EPC Index')]/following-sibling::td//text()").get()
        if energy:
            energy_label = energy.split(",")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label) )
        
        lat_lng = response.xpath("//script[contains(.,'lat')]/text()").get()
        if lat_lng:
            lat_lng = lat_lng.split("infoPanden = [[")[1].split("]]")[0]
            lat = lat_lng.split('"",')[1].split(',')[0].strip()
            lng = lat_lng.split('"",')[1].split(',')[1].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        desc = "".join(response.xpath("//p[@class='description']//text()").getall())
        if desc:
            desc = desc.replace("\u20ac","")
            item_loader.add_value("description", desc)
        
        square_meters = response.xpath("//tr/td[contains(.,'Bewoonbare')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        elif "oppervlakte van" in desc:
            square_meters = desc.split("oppervlakte van")[1].split('m')[0].replace("+-","").strip()
            item_loader.add_value("square_meters", square_meters)
        
        if "verdieping" in desc.lower():
            floor = desc.lower().split("verdieping")[0].strip().split(" ")[-1]
            if "eerste" in floor:
                item_loader.add_value("floor", "first")
            else:
                floor = floor.replace("ste","").replace("e","")
                if floor.isdigit():
                    item_loader.add_value("floor", floor)
        
        if "Syndiek:" in desc:
            utilities = desc.split("Syndiek:")[1].split("/")[0].replace("+","").strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        
        
        images = [x for x in response.xpath("//div[contains(@class,'slick_thumbs')]//div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Immo Roba")
        item_loader.add_value("landlord_phone", "09 388 53 53")
        item_loader.add_value("landlord_email", "info@immoroba.be")
        
        
        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label