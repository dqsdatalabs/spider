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
    name = 'desalas_comes'
    execution_type='testing'
    country='spain'
    locale='es'
    external_source='Desalas_PySpider_spain_es'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.desalas.com/es/search?operation=2&newtype%5B0%5D=8&minprice=2001&maxprice=30000"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='results']//li[contains(@class,'offerNoPrivate')]"):
            url_item = item.xpath(".//a[contains(.,'Ver más')]/@href").extract_first()
            follow_url = response.urljoin(url_item)
            yield Request(follow_url, callback=self.populate_item)
        pagination = response.xpath("//nav/a[@rel='next']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//div[@id='cover']//img/@alt").get()
        if dontallow and "vendido" in dontallow.lower():
            return 
        
        title = response.xpath("//h1//text()").get()
        if title: 
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        prop = "".join(response.xpath("//section[@id='offerdetails']/ul/li[1]/text()").extract())
        if prop:
            if "Ático" in prop or "Flat" in prop:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            elif "Chalet" in prop or "Piso" in prop or "Bajo" in prop:
                property_type = "house"
                item_loader.add_value("property_type", property_type)
            else: return
        
        item_loader.add_value("external_link", response.url)
        
        bathroom_count = response.xpath("//h3[contains(.,'CARACTERÍSTICAS')]/..//text()[contains(.,'Baños')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('Baños')[0].strip())
        else:
            bathroom_count = response.xpath("//h3[contains(.,'PROPERTY')]/..//text()[contains(.,'Bathroom')]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split('Bathroom')[0].strip())
        
        external_id=response.xpath("//span[@id='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip()) 
        
        desc="".join(response.xpath("//div[@id='offerInfo']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)

        city = response.xpath("//script[contains(.,'addressLocality')]/text()").get()
        if city:
            city = city.split('"addressRegion": "')[1].split('"')[0].strip()
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//script[contains(.,'addressLocality')]/text()").get()
        if zipcode:
            zipcode = zipcode.split('"postalCode": "')[1].split('"')[0].strip()
            item_loader.add_value("zipcode", zipcode)

        energy_label = response.xpath("//li[contains(.,'Certificado energ')]/text()[3]").get()
        if energy_label:
            energy_label = energy_label.split(':')[-1].strip()
            if not 'proceso' in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li[contains(.,'Planta')]/text()[3]").get()
        if floor:
            floor = floor.split(':')[-1].strip()
            item_loader.add_value("floor", floor)

        if desc:
            if 'amueblado' in desc.lower():
                item_loader.add_value("furnished", True)
            if 'ascensor' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'terraza' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'piscina' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'lavadora' in desc.lower():
                item_loader.add_value("washing_machine", True)
            
        rent="".join(response.xpath("//span[@class='priceProduct']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent+"€")
        
        address = "".join(response.xpath("//ul[@class='list-unstyled']/li[2]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        room_count="".join(response.xpath("//ul[@class='list-unstyled']/li[contains(.,'Dormitorio')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        elif "dormitorios" in desc:
            room=desc.split("dormitorios")[0].strip().split(" ")[-1]
            if room.isdigit():
                item_loader.add_value("room_count", room)
        
        square_meters="".join(response.xpath("//ul[@class='list-unstyled']/li[contains(.,'m2')]/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        desc="".join(response.xpath("//div[@id='offerInfo']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
            
        latitude_longitude=response.xpath("//script[contains(.,'myLatlng')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split("LatLng(")[1].split(",")[0]
            lng=latitude_longitude.split("LatLng(")[1].split(",")[1].split(")")[0]
            if lat or lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)

        images =[x for x in response.xpath(
            "//figure/a/ul[contains(@class,'slideHighlight list-unstyled')]//img/@src"
            ).getall()]
        image=response.xpath("//img[@class='cover']/@src").get()
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        elif image:
            item_loader.add_value("images", image)
            item_loader.add_value("external_images_count", str(len(image)))

        
        item_loader.add_value("landlord_name","DE SALAS")
        item_loader.add_value("landlord_phone","91 266 00 57")
        
        yield item_loader.load_item()