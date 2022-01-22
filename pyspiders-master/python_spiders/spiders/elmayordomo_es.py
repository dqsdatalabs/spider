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
import re
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'elmayordomo_es'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.elmayordomo.es/browser.php?ultProp=&tinm=2&tof=&loc=0&locv=&prov=0&vac=&alq=&neg=&internacional=0&ref=&tofn=3&tofnBA=&int_pais=&tinmBA=&int_estado=&provBA=&localidad_int=&locBA=&area=&distrito=&dormitoriosmin=&dormitoriosmax=&banos=&banosMax=&eti=&tcasa=&precio2=&precio1=&supParDesde=&playa=&supConstDesde=&fecha=Entrada+...&fecha2=Salida+...&lujo=&barrio=&jardin=&piscina=&garaje=&alarma=&golf=&ascensor=&wifi=&aacond=&chimenea=&trastero=&amueblado=&adepor=&exacta=&rebajado=&banco=&estudiantes=&mar=&alqTemporadas=&ordentinmueble=&ordenprecio=1&ordenhabitaciones=&ordenlocalidad=&ordencreacion=&ordenarpor=1&p=0&idi=es&ba=&ba2=&br=1&vistaLista=0&cacheClr=0&nocomision=",
                    "http://www.elmayordomo.es/browser.php?idi=es&br=1&lastSearch=1&internacional=0&ref=&tofn=3&tinm=8&prov=0&loc=0&fecha=Entrada+&fecha2=&dormitoriosmin=&dormitoriosmax=&precio1=&precio2=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://www.elmayordomo.es/browser.php?idi=es&br=1&lastSearch=1&internacional=0&ref=&tofn=3&tinm=16&prov=0&loc=0&fecha=Entrada+&fecha2=&dormitoriosmin=&dormitoriosmax=&precio1=&precio2=",
                    "http://www.elmayordomo.es/browser.php?ultProp=&tinm=64&tof=&loc=0&locv=&prov=0&vac=&alq=&neg=&internacional=0&ref=&tofn=3&tofnBA=&int_pais=&tinmBA=&int_estado=&provBA=&localidad_int=&locBA=&area=&distrito=&dormitoriosmin=&dormitoriosmax=&banos=&banosMax=&eti=&tcasa=&precio2=&precio1=&supParDesde=&playa=&supConstDesde=&fecha=Entrada+...&fecha2=Salida+...&lujo=&barrio=&jardin=&piscina=&garaje=&alarma=&golf=&ascensor=&wifi=&aacond=&chimenea=&trastero=&amueblado=&adepor=&exacta=&rebajado=&banco=&estudiantes=&mar=&alqTemporadas=&ordentinmueble=&ordenprecio=1&ordenhabitaciones=&ordenlocalidad=&ordencreacion=&ordenarpor=1&p=0&idi=es&ba=&ba2=&br=1&vistaLista=0&cacheClr=0&nocomision=",

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

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//td[@class='num']/following-sibling::td[1]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            if "&p=" in response.url:
                url = response.url.split("&p=")[0] + f"&p={page}" + (response.url.split("&p=")[1])[response.url.split("&p=")[1].find("&"):]
                yield Request(
                    url=url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page":page+1}
                )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Elmayordomo_PySpider_"+ self.country + "_" + self.locale)
        title=response.xpath("//table//h1/text()").extract_first()
        if title:
            if "," in title:
                item_loader.add_value("title", title.split(",")[0])
                rent= title.split(",")[1]
                if rent :     
                    rent= rent.split("/")[0]      
                    item_loader.add_value("rent_string", rent.strip())
            else:
                item_loader.add_value("title", title)
        item_loader.add_value("currency", "EUR")

        address = "".join(response.xpath("//table[@class='tdetalle']//td[contains(.,'Zona')]//text()[not(contains(.,'Zona')) and not(contains(.,'Amueblado')) and not(contains(.,'Sup')) and not(contains(.,'m2')) and not(contains(.,'Plantas')) and not(contains(.,'Planta'))]").extract())
        if address and "," in address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address))
            item_loader.add_value("zipcode", address.split(",")[0].strip())
        else:
            address = "".join(response.xpath("//table[@class='tdetalle']//tr[./td[contains(.,'En Área Urbana')]]/following-sibling::tr[1]//text()[not(contains(.,'Zona')) and not(contains(.,'Amueblado'))]").extract())
            if address:
                item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
                item_loader.add_value("zipcode", address.strip().split(",")[0].strip())
            else:
                address = "".join(response.xpath("//table[@class='tdetalle']//tr[./td[contains(.,'Plantas')]]/following-sibling::tr[1]//text()[not(contains(.,'Zona')) and not(contains(.,'Amueblado'))]").extract())
                if address:
                    item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
                    item_loader.add_value("zipcode", address.strip().split(",")[0].strip())
            
        city = response.xpath("//table[@class='tdetalle']//td[contains(.,'Zona:')]//text()[contains(.,'Zona:')]").get()
        if city:
            item_loader.add_value("city", city.split(":")[1].strip().split(",")[0])

        ex_id=response.xpath("//table//h1/small/text()").extract_first()
        if ex_id:
            external_id=ex_id.split(":")[1].split(")")[0]
            item_loader.add_value("external_id", external_id.strip())
        
        square_meters = response.xpath("//tr[td/text() ='Sup. Cons.:']/td[2]/text()").extract_first()
        if square_meters is not None:            
            
            square_meters = square_meters.split(",")[0].strip()
            # square_meters =square_meters.split("m")[0]
            item_loader.add_value("square_meters", str(square_meters))
        else:
            square_meters = response.xpath("//tr[td/text() ='Sup. Parcela:']/td[2]/text()").extract_first()      
            if square_meters is not None:
                # square_meters =square_meters.split("m")[0]
                square_meters = square_meters.split(",")[0]
                item_loader.add_value("square_meters", str(square_meters))
            
        features = "".join(response.xpath("//table[@class='tdetalle']//tr[contains(.,'Características')]/following-sibling::tr//text()").extract())
        if features:
            features=re.sub("\s{2,}", " ", features)
            if "Terraza" in features:
                item_loader.add_value("terrace", True)
            if "Garaje" in features:
                item_loader.add_value("parking", True)
            if "Piscina" in features:
                item_loader.add_value("swimming_pool", True)
            if "Ascensor" in features:
                item_loader.add_value("elevator", True)
            if "Balcón" in features:
                item_loader.add_value("balcony", True)
            if "Lavavajillas" in features:
                item_loader.add_value("dishwasher", True)
            if "Lavadora" in features:
                item_loader.add_value("washing_machine", True)
            if "Admite mascotas" in features:
                item_loader.add_value("pets_allowed", True)
                 
        floor = response.xpath("//td[contains(.,'Plantas')]/text()[normalize-space()]").extract_first()
        if floor :  
            item_loader.add_value("floor",floor.split('Planta')[0].replace("ª","").strip())
        
        bathroom = response.xpath("//table[@class='tdetalle']//tr[contains(.,'Características')]/following-sibling::tr//text()[contains(.,'Baños')]").extract_first()
        if bathroom :  
            bathroom = bathroom.strip().split(" ")[1].strip()  
            item_loader.add_value("bathroom_count",bathroom.strip())
        
        room = response.xpath("normalize-space(//table[@class='tdetalle']//tr[contains(.,'Características')]/following-sibling::tr//text()[contains(.,'Dormitorio')])").extract_first()
        if room :  
            room = room.strip().split(" ")[1].strip()  
            item_loader.add_value("room_count",room.strip())  

        furnished = response.xpath("//td[contains(.,'Amueblado')]/b/text()[normalize-space()]").extract_first()  
        if furnished:
            item_loader.add_value("furnished", True)

        desc = "".join(response.xpath("//table[@class='tdetalle']//tr[contains(.,'Características')]/following-sibling::tr//text()").extract())
        if desc :
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))

        energy_label=response.xpath("//table[@class='tdetalle']//tbody[contains(.,'energética')]//tr[@class='v4']//text()[normalize-space()][.!='En trámite']").extract_first()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip() )
      
        images=[response.urljoin(x) for x in response.xpath("//div[@id='links']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
       
        latitude = response.xpath("//span[@class='latitude']/text()").get()
        longitude = response.xpath("//span[@class='longitude']/text()").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "El Mayordomo Inmobiliaria y Servicios")
        item_loader.add_value("landlord_phone", "956 379 072") 
        item_loader.add_value("landlord_email", "info@elmayordomo.es")   



        yield item_loader.load_item()

        
       

        
        
          

        

      
     