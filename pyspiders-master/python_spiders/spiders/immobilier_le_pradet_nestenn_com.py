# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
class MySpider(Spider):
    name = 'immobilier_le_pradet_nestenn_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immobilier_Le_Pradet_Nestenn_PySpider_france"
    def start_requests(self):
        start_urls = [
            {"url": "https://immobilier-le-pradet.nestenn.com/?prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"},
            {"url": "https://immobilier-le-pradet.nestenn.com/?prestige=0&action=listing&transaction=Location&list_ville=&list_type=Maison&type=Maison&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "house"},
            {"url": "https://immobilier-le-pradet.nestenn.com/?prestige=0&action=listing&transaction=Location&list_ville=&list_type=Studio&type=Studio&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "studio"}
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='bienParent1']/div[@id='annonce_entete_right']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.meta.get('property_type')
        prp_type_studio = response.xpath("//h2[@id='titre']/text()[contains(.,'Appartement')]").get()
        if prp_type_studio:
            property_type = "apartment"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)     
        title = response.xpath("//h2[@id='titre']/text()").get()
        item_loader.add_value("title", title)
        
        rent = "".join(response.xpath("//div[@class='content_prix']/text()").getall())
        if rent:
            if " semaine" in rent:
                price = rent.split("€")[0].strip().split(" ")[-1].replace(" ","").strip()
                item_loader.add_value("rent", str(int(price)*4))
            else:
                price = rent.split("€")[0].strip().replace(" ","")
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div/img[contains(@src,'chambre')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//div/img[contains(@src,'piece')]/following-sibling::div/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])
                
        bathroom_count = response.xpath("//div/img[contains(@src,'salle')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//div/img[contains(@src,'surface')]/following-sibling::div/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        address = response.xpath("//div[a[@property='item']][last()]/a//text()").get()
        if address:
            zipcode = address.split("(")[-1].split(")")[0]
            item_loader.add_value("zipcode", zipcode.strip())
            address = " ".join(address.split("(")[0].strip().split(" ")[2:])
            if address:
                item_loader.add_value("address", address.capitalize())
                item_loader.add_value("city", address.capitalize())
        
        elevator = response.xpath("//div/img[contains(@src,'ascenseur')]/following-sibling::div/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div/img[contains(@src,'terrasse')]/following-sibling::div/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div/img[contains(@src,'parking')]/following-sibling::div/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div/img[contains(@src,'balcon')]/following-sibling::div/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        swimming_pool = response.xpath("//div/img[contains(@src,'piscine')]/following-sibling::div/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        floor = response.xpath("//div/img[contains(@src,'etage')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        energy_label = response.xpath("//div[@id='consoEner']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        external_id = response.xpath("//div[@id='ref']/text()").get()
        if external_id:
            external_id = external_id.split("Réf :")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        desc = " ".join(response.xpath("//section[@id='annonce_detail']/div/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "disponible le" in desc.lower():
            available_date = desc.lower().split("disponible le")[1].split("logement")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'center:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center: [')[1].split(',')[0]
            longitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        deposit = response.xpath("//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie :')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", int(float(deposit)))
        
        images = [x for x in response.xpath("//div[@class='slider_bien']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Immobilier sur Le Pradet")
        item_loader.add_value("landlord_phone", "04 94 00 19 20")
        item_loader.add_value("landlord_email", "lepradet@nestenn.com")
        yield item_loader.load_item()