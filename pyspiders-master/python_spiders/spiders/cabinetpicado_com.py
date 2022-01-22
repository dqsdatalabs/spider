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
    name = 'cabinetpicado_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'

    def start_requests(self):

        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "1"
            },
        ]

        for item in start_urls:
            formdata = {
                "type[]": item["type"],
                "price": "",
                "reference": "",
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
            yield FormRequest(
                "https://www.cabinetpicado.com/fr/locations",
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type":item["property_type"]
                })
       


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='buttons']//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.cabinetpicado.com/fr/locations/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            )
        else:
            formdata = {
                "type[]": "2",
                "price": "",
                "reference": "",
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
            yield FormRequest(
                "https://www.cabinetpicado.com/fr/locations",
                callback=self.parse,
                formdata=formdata,
                meta={
                    "property_type": "house",
                    'dont_merge_cookies': True
                })

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cabinetpicado_PySpider_france")

        title = response.xpath("//div[@class='titles']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
            zipcode = title.split(" -")[-1].strip()
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
        
        rent=response.xpath("//article/div/ul/li[contains(.,'€')]/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
        
        square_meters=response.xpath("//div[contains(@class,'summary')]/ul/li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        else:
            if title and "m²" in title:
                for i in range(len(title.split(" "))):
                    if "m²" in title.split(" ")[i].lower():
                        item_loader.add_value("square_meters", title.split(" ")[i].replace("m²","").strip())
                        break
        
        room_count = response.xpath('//li[@class="alt"]/span/text()').get()
        if room_count:
            item_loader.add_value('room_count', room_count[0])

        bathroom_count = response.xpath("//article/div/ul/li[contains(.,'salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('salle')[0].strip())

        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//li[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        furnished = response.xpath("//li[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[2].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[2].split(',')[1].split(']')[0].strip()           
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address=response.xpath("//article/div/h2/text()[last()]").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.strip().split(" ")[-1].strip()
            if city.isalpha():
                item_loader.add_value("city",city)
            else:
                city = address.strip().split(" ")[0].strip()
                item_loader.add_value("city",city)
                     
        external_id=response.xpath("//article/div/ul/li[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())

        desc="".join(response.xpath("//p[@id='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[contains(@class,'resizePicture')]/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        name=response.xpath("//p[contains(@class,'serName')]/strong/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        phone=response.xpath("//span[contains(@class,'phone')]/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("+",""))
        email=response.xpath("//span[contains(@class,'mail')]/a/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        
        floor=response.xpath("//div[contains(@class,'summary')]/ul/li[contains(.,'Etage')]/span/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.split('/')[0].strip())))
            
        utilities=response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'Charges') and not(contains(.,'copropriété'))]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip().replace(' ', ''))
        elif not utilities:
            utilities=response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'Honoraires d')]/span/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[0].strip().replace(' ', ''))
        
        deposit=response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'garantie')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip().replace(' ', ''))
        else:
            deposit = "".join(response.xpath("//p[contains(@id,'description')]//text()[contains(.,'Dépôt de garantie :')]").getall())
            if deposit:
                deposit = deposit.split(":")[1].replace("€","").strip()
                item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//img[contains(@alt,'Consommation')]/@src").get()
        if energy_label:
            energy_label = int(float(energy_label.split('/')[-1].strip().replace('%2C', '.')))
            if energy_label <= 50:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 51 and energy_label <= 90:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 91 and energy_label <= 150:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 151 and energy_label <= 230:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 231 and energy_label <= 330:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 331 and energy_label <= 450:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 451:
                item_loader.add_value("energy_label", 'G')
        
        elevator=response.xpath("//div[contains(@class,'services')]/ul/li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
            
        garage=response.xpath("//div[contains(@class,'areas')]/ul/li[contains(.,'Garage')]/text()").get()
        parking=response.xpath("//div[contains(@class,'proximities')]/ul/li[contains(.,'Parking')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking",True)

        yield item_loader.load_item()

