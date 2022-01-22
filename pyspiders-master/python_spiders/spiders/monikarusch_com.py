# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'monikarusch_com'
    execution_type='testing'
    country='spain'
    locale='es'

    def start_requests(self):
        start_urls = [
            {
                "r_type" : [
                    1,
                    3,
                ],
                "property_type" : "apartment"
            },
            {
                "r_type" : [
                    2,
                ],
                "property_type" : "house"
            },
            

        ]
        for url in start_urls:
            for item in url.get("r_type"):
                payload = "{\"filtro\":{\"zonas\":[],\"subzonas\":[],\"idioma\":\"es\",\"referencia\":null,\"tipoOperacion\":2,\"tipoPropiedad\":" + str(item) + ",\"zonaWeb\":null,\"poblacion\":null,\"zona\":null,\"subzona\":null,\"superficieMax\":null,\"superficieMin\":0,\"habitaciones\":null,\"banyos\":null,\"precioMax\":null,\"precioMin\":0,\"aireAcondicionado\":null,\"amueblado\":null,\"obraNueva\":null,\"calefaccion\":null,\"parking\":null,\"terraza\":null,\"piscina\":null,\"portero\":null,\"ascensor\":null}" + "}"
                yield Request("https://monikarusch.com/backend/index.php?module=search&req=get-viviendas",
                            callback=self.parse,
                            method="POST",
                            body=payload,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        for item in data["response"]["viviendas"]:
            url = "https://monikarusch.com/es/" + item["prefix"] + item["alias"] + f"-{item['id']}"
            lat,lng = item["latitud"], item["longitud"]
            title = item["nombre"]
            room_count = item["habitaciones"]
            square_meters = str(item["superficie"])
            bathroom_count=str(item["banyos"])
            external_id = item["referencia"]
            energy_label = item["certificado_energetico"]
            rent = item["precio"]
            address = item["ubicacion_google"]
            parking = item["parking"]
            terraza = item["terraza"]
            ascensor = item["ascensor"]
            swimming_pool = item["piscina"]
            furnished = item["amueblado"]
            image = item["thumbnails"]
            yield Request(
                url,
                callback=self.populate_item,
                meta={
                    'property_type': response.meta.get('property_type'),
                    'lat' : lat,
                    'lng' : lng,
                    'title' : title,
                    'room_count' : room_count,
                    'square_meters' : square_meters,
                    'bathroom_count' : bathroom_count,
                    'external_id' : external_id,
                    'energy_label' : energy_label,
                    'rent' : rent,
                    'terraza' : terraza,
                    'swimming_pool' : swimming_pool,
                    'furnished' : furnished,
                    'ascensor' : ascensor,
                    'image' : image,
                    'address' : address,
                    'parking' : parking,
                    
                }
            )
            
    def get_items(self, response):
        data = json.loads(response.body)
        desc = data['response']
        description = ""
        for item in desc:
            description += item['descripcion_larga'] + " "
        description = description.strip()

        item_loader = response.meta.get("item_loader")

        item_loader.add_value("description", description)
        if 'lavadora' in description.lower():
            item_loader.add_value("washing_machine", True)
        if 'lavavajillas' in description.lower():
            item_loader.add_value("dishwasher", True)
        if 'balcón' in description.lower():
            item_loader.add_value("balcony", True)
        if 'ascensor' in description.lower():
            item_loader.add_value("elevator", True)
        elif 'sin ascensor' in description.lower():
            item_loader.add_value("elevator", False)

        yield item_loader.load_item()

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Monikarusch_PySpider_"+ self.country + "_" + self.locale)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        energy_label = response.meta.get('energy_label')
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("latitude", str(response.meta.get('lat')))
        item_loader.add_value("longitude", str(response.meta.get('lng')))
        item_loader.add_value("title", response.meta.get('title'))
        
        room = response.meta.get('room_count')
        if room !=0:
            item_loader.add_value("room_count", room)
        
        bathroom=response.meta.get('bathroom_count')
        if bathroom !=0:
            item_loader.add_value("bathroom_count", bathroom)
        
        item_loader.add_value("square_meters", response.meta.get('square_meters'))
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("external_id", response.meta.get('external_id'))
        item_loader.add_value("rent", response.meta.get('rent'))
        image = []
        images = response.meta.get('image')
        for i in images:
            image.append(i)
            item_loader.add_value("images", image)

        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1])
        
        elevator = str(response.meta.get('ascensor'))
        if elevator != "0":
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)
        
        swimming_pool = str(response.meta.get('swimming_pool'))
        if swimming_pool != "0":
            item_loader.add_value("swimming_pool", True)
        else:
            item_loader.add_value("swimming_pool", False)

        furnished = str(response.meta.get('furnished'))
        if furnished != "0":
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        parking = str(response.meta.get('parking'))
        if parking != "0":
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        terraza = str(response.meta.get('terraza'))
        if terraza != "0":
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)
            
        item_loader.add_value("landlord_name", "MONIKA RUSH REAL ESTATE AGENCY")

        landlord_phone = response.xpath("//text()[contains(.,'Tel ')]/following-sibling::a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        landlord_email = "".join(response.xpath("//a/@href[contains(.,'mail')]").extract())
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.replace("mailto:",""))
        
        url_id = response.url.split('-')[-1].strip().strip('/')
        yield Request(f"https://monikarusch.com/backend/index.php?module=properties&req=get-house-description&id={url_id}&id_idioma=1", callback=self.get_items, meta={"item_loader": item_loader}) 
        
        
       

        
        
          

        

      
     