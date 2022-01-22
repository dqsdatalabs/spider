# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider 
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
from datetime import datetime 
from datetime import date
import dateparser
import re
class MySpider(Spider): 
    name = 'agencevauban_com'
    execution_type='testing'
    country='france'
    locale='fr' # LEVEL 1

    def start_requests(self):
        start_urls = ["http://agencemourey.com/biens"]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["biens"]:
            if item["bien_type_vente"] == "location":   
                url = "http://agencemourey.com/bien/"+str(item["bien_id"])
                property_type = item["bien_type_id"]
                if property_type == 2:
                    property_type = 'apartment'
                elif property_type == 1:
                    property_type = 'house'
                else:
                    property_type = 'pass'
                if property_type != 'pass':
                    yield Request(url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agencevauban_PySpider_"+ self.country + "_" + self.locale)
 
 

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = title.replace("\n"," - ").replace("- Agence Mourey","")
            item_loader.add_value("title", title)
            address = title.split(' - ')[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        square_meters = response.xpath("//p[contains(.,'Surface')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[-1].split('m')[0].strip()
            square_meters = square_meters.replace('\xa0', '').replace(',', '.').replace(' ', '.').strip()
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters) 
  
        room_count = response.xpath("//p[contains(.,'Nombre de pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(':')[-1])
        else:
            room_count1= response.xpath("//p[contains(.,'Nombre de chambre')]/text()").get()
            if room_count1:
                item_loader.add_value("room_count",room_count1.split(':')[-1])
            

        bathroom_count = response.xpath("//p[contains(.,'Nombre de salle')]/text()").get()
        if bathroom_count:
            bathroom_count=re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        
        available_date = response.xpath("//div/p[strong[text()='Description']]/following-sibling::p//text()[contains(.,'Disponible le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('disponible le')[-1].split('-')[0], date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:          
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        rent = response.xpath("//p[contains(.,'Prix de location')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.split(".")[0].replace(",","."))
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//p[contains(.,'Réference')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        remove=item_loader.get_output_value("external_id")
        if remove=="L010":
            return
        description = " ".join(response.xpath("//div/p[strong[text()='Description']]/following-sibling::p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='slider']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        utilities = response.xpath("//div/text()[contains(.,'Charges mensuelles')][not(contains(.,': 0 €'))]").get()
        if utilities:
            utilities = utilities.split(':')[-1].split('€')[0].replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("utilities", utilities)
        energy_label = response.xpath("//p[contains(.,'Consommation:')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(':')[-1].strip())
       
        item_loader.add_value("landlord_phone", "03 81 83 89 94")
        item_loader.add_value("landlord_name", "Agence Mourey")
        item_loader.add_value("landlord_email", "location@agencemourey.com")
        parking = response.xpath("//p[contains(.,'Garage :')]/text()").get()
        if parking:
            parking = parking.split(':')[-1].strip()
            if parking.strip() == '0':
                item_loader.add_value("parking", False) 
            else:
                item_loader.add_value("parking", True) 
        yield item_loader.load_item()
