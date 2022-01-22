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
import math
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'nextdoorbcn_com' 
    execution_type='testing'
    country='spain'
    locale='es'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.nextdoorbcn.com/results/?id_tipo_operacion=2&type=1%2C18%2C21%2C22%2C29%2C26&dt=&dormitorios_min=&precio_max=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.nextdoorbcn.com/results/?id_tipo_operacion=2&type=3&dt=&dormitorios_min=&precio_max=",

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

       
        for item in response.xpath("//div[@class='swiper-slide']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
         

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        if "https://www.nextdoorbcn.com/results/?id_tipo_operacion" not in response.url:
            item_loader.add_value("external_link", response.url)
            item_loader.add_xpath("title", "//div[@class='headerLeft']/h1/text()")
            item_loader.add_value("external_source", "Nextdoorbcn_PySpider_"+ self.country + "_" + self.locale)
            item_loader.add_xpath("external_id", "//div[@class='headerLeft']/p/span/text()")
            
            province = "".join(response.xpath("//div[@class='detallesFicha']/ul/li[contains(.,'Provincia')]/text()").extract())
            zona = "".join(response.xpath("//div[@class='detallesFicha']/ul/li[contains(.,'Zona')]/text()").extract())
            if province or zona:
                item_loader.add_value("address", zona.lstrip().rstrip() + " " + province.lstrip().rstrip())
                item_loader.add_value("city", zona.lstrip().rstrip())
            citycheck=item_loader.get_output_value("city") 
            if citycheck=="":
                item_loader.add_value("city",zona.lstrip().rstrip() + " " + province.lstrip().rstrip())
            
            square_meters = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Útil')]/text()[normalize-space()]").extract_first()
            if square_meters :            
                square_meters = math.ceil(float(square_meters.split("m")[0]))
                item_loader.add_value("square_meters", str(square_meters))
            else:
                square_meters = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Construida')]/text()[normalize-space()]").extract_first()
                if square_meters :
                    square_meters = math.ceil(float(square_meters.split("m")[0]))
                    item_loader.add_value("square_meters", str(square_meters))

            room = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Habitaciones: ')]/text()[normalize-space()]").extract_first()
            if room: 
                item_loader.add_value("room_count",room)
            
            elif not room:
                room1=response.xpath("//h3/i[@class='fa fa-home']/following-sibling::text()").get()
                if room1:
                    roomcount=room1.split("DORMITORIO")[0]
                    if roomcount:
                        roomcount=re.findall("\d+",roomcount)
                        if roomcount:
                            item_loader.add_value("room_count",roomcount[-1])

            
            bathroom = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Baños')]/text()[normalize-space()]").get()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom.strip())
            
            floor = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Planta')]/text()[normalize-space()]").extract_first()
            if floor :  
                item_loader.add_value("floor", floor.replace("º","").strip())

            rent = response.xpath("//p[@class='precio']//text()").extract_first()
            if rent :     
                rent= rent.split(":")[1].split("/")[0]      
                item_loader.add_value("rent_string", rent.strip())
            
        
            desc = "".join(response.xpath("//div[@id='descripcionFicha2']/p//text()").extract())
            if desc :
                item_loader.add_value("description", desc.strip())

            elevator = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Ascensor')]/text()[normalize-space()]").extract_first()
            if elevator :
                item_loader.add_value("elevator", True)
            else:
                if "Ascensor" in desc or "ascensor" in desc:
                    item_loader.add_value("elevator", True)
        
            terrace = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Terraza')]/text()[normalize-space()]").extract_first()
            if terrace :
                item_loader.add_value("terrace", True)
            else:
                if "Terraza" in desc or "terraza" in desc:
                    item_loader.add_value("terrace", True)
            
            furnished = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Amueblado')]/text()[normalize-space()]").extract_first()  
            if furnished:
                item_loader.add_value("furnished", True)
            else:
                if "Amueblado" in desc or "amueblado" in desc:
                    item_loader.add_value("furnished", True)

            swimming_pool = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Piscina')]/text()[normalize-space()]").extract_first()
            if swimming_pool :
                item_loader.add_value("swimming_pool", True)
            else:
                if "piscina" in desc or "Piscina" in desc:
                    item_loader.add_value("swimming_pool", True)

            parking = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Parking') or contains(.,'Garaje')]/text()[normalize-space()]").extract_first()
            if parking :
                item_loader.add_value("parking", True)
            else:
                if "parking" in desc or "garaje" in desc:
                    item_loader.add_value("parking", True)
                    
            energy_label=response.xpath("//div[@class='detallesFicha']//li[contains(.,'energética (consumo)') or contains(.,'energética (emisiones)')]/text()[normalize-space()]").extract_first()
            if energy_label  :
                energy_label=energy_label.strip().split(" ")[0]
                if energy_label:
                    item_loader.add_value("energy_label", energy_label)
        
            img=response.xpath("//div[@class='fotorama']/a/@data-thumb").extract() 
            if img:
                images=[]
                for x in img:
                    images.append(x)
                if images:
                    item_loader.add_value("images",  list(set(images)))

            pets_allowed = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Admite mascotas')]/text()[normalize-space()]").get()
            if pets_allowed:
                item_loader.add_value("pets_allowed", True)
            else:
                if "Admite mascotas" in desc or "admite mascotas" in desc:
                    item_loader.add_value("pets_allowed", True)
            
            
            latitude = response.xpath("//div[@id='mapa']/@data-lat").get()
            longitude = response.xpath("//div[@id='mapa']/@data-lng").get()
            if latitude and longitude:
                latitude = latitude.strip().replace(',', '.')
                longitude = longitude.strip().replace(',', '.')
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
            latcheck=item_loader.get_output_value("latitude")
            if not latcheck:
                lat=response.xpath("//div[@class='alquiler punto_mapa']/@data-lat").get()
                item_loader.add_value("latitude",lat)
            lngcheck=item_loader.get_output_value("longitude")
            if not lngcheck:
                lng=response.xpath("//div[@class='alquiler punto_mapa']/@data-lng").get()            
                item_loader.add_value("longitude",lng)

            item_loader.add_value("landlord_name", "NEXT DOOR REAL ESTATE")
            item_loader.add_value("landlord_phone", "932206262") 
            item_loader.add_value("landlord_email", "info@nextdoorbcn.com") 

            if "balcón" in desc or "Balcón" in desc:
                item_loader.add_value("balcony", True)
            
            if "lavavajillas" in desc or "Lavavajillas" in desc:
                item_loader.add_value("dishwasher", True)
            
            if "lavadora" in desc or "Lavadora" in desc:
                item_loader.add_value("washing_machine", True)
        
        

        yield item_loader.load_item()

        
       

        
        
          

        

      
     