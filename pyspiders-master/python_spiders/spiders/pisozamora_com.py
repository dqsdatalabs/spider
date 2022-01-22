# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'pisozamora_com'
    execution_type='testing'
    country='spain'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pisozamora.com/alquiler.php?limtipos=2799,2899,2999,3399,4399&buscador=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.pisozamora.com/alquiler.php?limtipos=7599,3699,399,499,7599,3699&buscador=1",

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

        for item in response.xpath("//a[@class='irAfichaPropiedad']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title","//div[@class='fichapropiedad-tituloprincipal']/h1/text()")

        item_loader.add_value("external_source", "Pisozamora_PySpider_"+ self.country + "_" + self.locale)

        latitude_longitude = response.xpath("//script[contains(.,'objeto de la')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitud":')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('"altitud":')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        address = response.xpath("//ul[@class='fichapropiedad-listadatos']/li[./span[contains(.,'Area / City')]]/span[@class='valor']/text()").get()
        if address:
            item_loader.add_value("address", address)

        square_meters = response.xpath("//span[contains(.,'Built Surface')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'Bedrooms')]/following-sibling::span/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[contains(.,'Bathroom')]/following-sibling::span/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_xpath("floor", "//span[contains(.,'Level')]/following-sibling::span/text()")

        utilities = response.xpath("//span[contains(.,'Community fees')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        rent = response.xpath("//div[@class='fichapropiedad-precio']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('.', '')
            item_loader.add_value("rent", rent)

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//span[contains(.,'Reference')]/following-sibling::span/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)
        
        desc="".join(response.xpath("//section[@id='fichapropiedad-bloquedescripcion']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        city = response.xpath("//section[@id='fichapropiedad-bloquedescripcion']/text()").get()
        if city:
            city = city.split('-')[1].split('(')[0].strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@id='fotosNormales']//ul//li/@cargafoto").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//b[contains(.,'Furniture')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        parking = response.xpath("//b[contains(.,'Garage')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)
        else:
            if "parking" in desc or "garage" in desc:
                item_loader.add_value("parking", True)

        elevator = response.xpath("//b[contains(.,'Elevator')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)
        else:
            if "elevator" in desc or "lift" in desc or "Lift" in desc:
                item_loader.add_value("elevator", True)

        balcony = response.xpath("//b[contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)
        else:
            if "balcony" in desc:
                item_loader.add_value("balcony", True)
        
        if "washing machine" in desc:
            item_loader.add_value("washing_machine", True)
        
        if "dishwasher" in desc:
            item_loader.add_value("dishwasher", True)

        terrace = response.xpath("//b[contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)
        else:
            if "terrace" in desc:
                item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//b[contains(.,'Pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)
        else:
            if "pool" in desc:
                item_loader.add_value("swimming_pool", True)

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

        landlord_name = response.xpath("//div[@class='pie-datosagencia']/ul/li[1]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='pie-datosagencia']/ul/li[4]/a[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//div[@class='pie-datosagencia']/ul/li[2]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()

        
        
          

        

      
     