# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import re
import dateparser

class MySpider(Spider):
    name = 'a2mimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            payload = {
                "nature": "2",
                "type[]": r_type,
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
            
            yield FormRequest(url="https://www.a2mimmo.fr/fr/recherche/",
                            callback=self.parse,
                            formdata=payload,
                            meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='buttons']//a[@class='button']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get('property_type')
        type_studio = response.xpath("//h2/text()[contains(.,'Studio')]").get()
        if type_studio:
            property_type = "studio"

        item_loader.add_value("property_type", property_type)  

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "A2mimmo_PySpider_"+ self.country + "_" + self.locale)

        title = "+".join(response.xpath("//div/ul[contains(.,'Ref')]/parent::div/h2/text()").getall())
        if title:
            item_loader.add_value("address", title.strip().split("+")[1])
            item_loader.add_value("city", title.strip().split("+")[1])
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.replace("+","")))
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('marker_map_2 = L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
                
        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Pièces')]/span/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        bathroom = response.xpath(
            "//div/ul[contains(.,'Ref')]/parent::div/ul/li[contains(.,'salle') or contains(.,'Salle')]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("salle")[0].strip())
        
        rent = response.xpath("//li[contains(text(),'Mois')]/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//li[contains(text(),'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split('.')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@id='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//section[@class='showPictures']/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//img[contains(@alt,'Consommation')]/@src").get()
        if energy_label:
            energy_label = float(energy_label.split('/')[-1].strip().replace('%2C', '.'))
            if energy_label > 0:
                if energy_label <= 50:
                    energy_label = 'A'
                elif energy_label >= 51 and energy_label <= 90:
                    energy_label = 'B'
                elif energy_label >= 91 and energy_label <= 150:
                    energy_label = 'C'
                elif energy_label >= 151 and energy_label <= 230:
                    energy_label = 'D'
                elif energy_label >= 231 and energy_label <= 330:
                    energy_label = 'E'
                elif energy_label >= 331 and energy_label <= 450:
                    energy_label = 'F'
                elif energy_label >= 451:
                    energy_label = 'G'
                item_loader.add_value("energy_label", energy_label)

        available_date = response.xpath("//li[contains(.,'Disponible le')]/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//li[contains(.,'Parking')]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//h2[.='Prestations']/following-sibling::ul/li[contains(.,'Meublé')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        washing = response.xpath("//div[@class='services details']//li[contains(.,'Lave-linge')]/text()").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//div[@class='services details']//li[contains(.,'Lave-vaisselle')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        floor = response.xpath("//li[contains(.,'Etage')]/span/text()").get()
        if floor:
            floor = floor.strip().split('/')[0].strip()
            item_loader.add_value("floor", floor)

        terrace = response.xpath("//li[contains(.,'Terrasse')]/span/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)
        
        swimming_pool = response.xpath("//h2[.='Prestations']/following-sibling::ul/li[contains(.,'Piscine')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        utilities = response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
            
        
        landlord_name = response.xpath("//p[@class='smallIcon userName']/strong/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//span[@class='phone smallIcon']/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//span[@class='mail smallIcon']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data