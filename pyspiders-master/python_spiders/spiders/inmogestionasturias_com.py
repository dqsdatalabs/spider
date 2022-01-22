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
import dateparser

class MySpider(Spider):
    name = 'inmogestionasturias_com'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.inmogestionasturias.com/find/?buy_op=rent&kind=flat&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=&max_price=&agency=&min_size=&max_size=&sort_by=&page=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.inmogestionasturias.com/find/?buy_op=rent&kind=chalet&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=&max_price=&agency=&min_size=&max_size=&sort_by=&page=1",
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
        for item in response.xpath("//div[@class='col-md-6 col-lg-4 house-wrapper']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(
                url=url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Inmogestionasturias_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        external_id=response.xpath("//div[@class='login-container']/div/h4/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        title=response.xpath("//div[@class='col-md-12']/div/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        room_count=response.xpath("//div[@class='row']/div/h4[contains(.,'dormitorio')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        square_meters=response.xpath("//div[@class='row']/div/h4[contains(.,'m2')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        rent=response.xpath("//div[@class='row']/div/p/span[contains(.,'€')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        address=response.xpath("//div[@class='col-md-12']/div/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        desc="".join(response.xpath("//div[@class='well']//p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        specials = " ".join(response.xpath("//ul[@class='list-unstyled']/../span/text()").getall()).strip()

        bathroom_count = response.xpath("//h4[contains(.,'baño')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0].strip())

        if desc:
            if 'disponible a partir del' in desc.lower():
                available_date = desc.split('disponible a partir del')[-1].split('!')[0].split('.')[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"], languages=['es'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            elif 'disponibilidad es a partir del' in desc.lower():
                available_date = desc.split('disponibilidad es a partir del')[-1].split('pudiendo')[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"], languages=['es'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            if 'no se admiten mascotas' in desc.lower():
                item_loader.add_value("pets_allowed", False)
            elif 'se admiten mascotas' in desc.lower():
                item_loader.add_value("pets_allowed", True)
            if 'piscina' in desc.lower() or 'piscina' in specials.lower():
                item_loader.add_value("swimming_pool", True)
            if 'garaje' in desc.lower() or 'garaje' in specials.lower():
                item_loader.add_value("parking", True)
            if 'lavavajillas' in desc.lower() or 'lavavajillas' in specials.lower():
                item_loader.add_value("dishwasher", True)
            if 'lavadora' in desc.lower() or 'lavadora' in specials.lower():
                item_loader.add_value("washing_machine", True)
            if 'ascensor' in desc.lower() or 'ascensor' in specials.lower():
                item_loader.add_value("elevator", True)
            if 'amueblado' in desc.lower() or 'amueblado' in specials.lower():
                item_loader.add_value("furnished", True)
            elif 'sin amueblar' in desc.lower():
                item_loader.add_value("furnished", False)
        
        floor=response.xpath("//div[@class='well']/ul/li[contains(.,'Planta')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
            
        terrace=response.xpath("//div[@class='well']/span[contains(.,'terraza')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
       
        energy_label=response.xpath("//div[@class='col-md-8']/p[contains(.,'Consumo energético:')]/text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)
        
        images=[x for x in response.xpath("//img[@class='masonry-item']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
       
        latitude_longitude=response.xpath("//script[contains(.,'L.map')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split("coords = [")[1].split(",")[0]
            lng=latitude_longitude.split("coords = [")[1].split(",")[1].split("]")
            if lat or lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
        
        item_loader.add_value("landlord_name","Inmogestión")
        item_loader.add_value("landlord_phone","684623277")
        item_loader.add_value("landlord_email","contacto.inmogestion@gmail.com")
        
       
        yield item_loader.load_item()

        
       

        
        
          

        

      
     