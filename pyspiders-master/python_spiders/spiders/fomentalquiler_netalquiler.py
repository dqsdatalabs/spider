# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import json
from geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'fomentalquiler_netalquiler'
    execution_type='testing'
    country='spain'
    locale='es'
    

    def start_requests(self):
        start_urls = [
            {"url": "https://www.propiedades.fomentto.net/alquiler.php?limtipos=199,399,7599,999,7599,199,399,199,399&buscador=1","property_type": "house"},
            {"url": "https://www.propiedades.fomentto.net/index.php?limtipos=399,499,7599,999,4599,6199,6299,6399,199,399,999,6199,7599,2799,2899,2999,3099,3299,3399,3499,4299,4399&limalquiler=1&av=1&buscador=1","property_type": "house"},
            {"url": "https://www.propiedades.fomentto.net/alquiler.php?limtipos=2799,3099,3399,3499,2799,3099,3399,3499,2799,3099,3399,3499&buscador=1","property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), 'res_url': url.get('url')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//article[@class='paginacion-ficha propiedad']//a[@class='irAfichaPropiedad']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        pagination = response.xpath("//ul[@id='paginacion-numPaginas']/li").extract()
        res_url = response.meta.get("res_url")
        if res_url:
            if pagination:
                for i in range(2, (len(pagination)+1)):
                    url = "{}&pag={}#modulo-paginacion".format(res_url, i)
                    yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if response.url == 'https://www.propiedades.fomentto.net':
            return

        item_loader.add_xpath("title", "//div[@class='fichapropiedad-tituloprincipal']/h1/text()")

        studio = response.xpath("//ul[@class='fichapropiedad-listadatos']/li[span[.='Type of property']]/span[2]/text()").extract_first()
        if "studio" in studio.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        bathroom_count = response.xpath("//li[contains(@class,'banyos')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        item_loader.add_value("external_source", "Fomentalquilernetalquiler_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("external_link", response.url)

        external_id=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Reference')]/span[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        room_count=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Bedrooms')]/span[2]/text()").get()
        room_c=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Type of property')]/span[2]/text()").get()
        if room_c=='Studio':
            item_loader.add_value("room_count", "1")
        elif room_count:
            item_loader.add_value("room_count", room_count)

        item_loader.add_xpath("bathroom_count", "//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Bathroom')]/span[2]/text()")

        
        square_meters=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Net Internal Area')]/span[2]/text()").get()
        surface=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Built Surface')]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        elif surface:
            item_loader.add_value("square_meters", surface.split("m")[0].strip())
        
        rent=response.xpath("//div[@class='fichapropiedad-precio']").get()
        if rent:
            item_loader.add_value("rent_string", rent)

        utilities = response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Community fees')]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)          

        desc="".join(response.xpath("//section[@id='fichapropiedad-bloquedescripcion']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Level')]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Furniture')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        elevator=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Elevator')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            if "elevator" in desc or "lift" in desc or "Lift" in desc:
                item_loader.add_value("elevator", True)
            
        terrace=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            if "terrace" in desc:
                item_loader.add_value("terrace", True)
            
        parking=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Parking')]/text()").get()
        garage=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Garage')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking", True)
        else:
            if "parking" in desc or "garage" in desc:
                item_loader.add_value("parking", True)
        
        swimming_pool=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        else:
            if "pool" in desc:
                item_loader.add_value("swimming_pool", True)
        
        if "balcony" in desc or "Balcony" in desc:
            item_loader.add_value("balcony", True)
        if "washing machine" in desc:
            item_loader.add_value("washing_machine", True)
        if "dishwasher" in desc:
            item_loader.add_value("dishwasher", True)
            
        images=[x for x in response.xpath("//div[@class='visorficha-miniaturas']/ul/li/@cargafoto").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        else:
            images = response.xpath("//div[contains(@class,'visorficha-principal')]/@cargafoto").get()
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
        
        A = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[2]//div[@class='flechaEficiencia']").get()
        B = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[3]//div[@class='flechaEficiencia']").get()
        C = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[4]//div[@class='flechaEficiencia']").get()
        D = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[5]//div[@class='flechaEficiencia']").get()
        E = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[6]//div[@class='flechaEficiencia']").get()
        F = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[7]//div[@class='flechaEficiencia']").get()
        G = response.xpath("//div[@id='fichapropiedad-certificacionenergetica']/table//tr[8]//div[@class='flechaEficiencia']").get()
        if A:
            item_loader.add_value("energy_label", 'A')
        elif B:
            item_loader.add_value("energy_label", 'B')
        elif C:
            item_loader.add_value("energy_label", 'C')
        elif D:
            item_loader.add_value("energy_label", 'D')
        elif E:
            item_loader.add_value("energy_label", 'E')
        elif F:
            item_loader.add_value("energy_label", 'F')
        elif G:
            item_loader.add_value("energy_label", 'G')

        address=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'City')]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("/")[1].strip())
       
        latitude_longitude=response.xpath("//script[contains(.,'listados.fichapropiedad')]/text()").get()
        if latitude_longitude:
            try:
                latitude = latitude_longitude.split('latitud":')[1].split(",")[0].strip('"')
                longitude = latitude_longitude.split('"altitud":')[1].split(",")[0].strip('"')
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
            except:
                pass
        
        phone=response.xpath("//span[@class='datosagencia-telf']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_name","FOMENTTO INMOBILIARIAS")
        item_loader.add_value("landlord_email","info@fomentoalquiler.net")
        
        yield item_loader.load_item()