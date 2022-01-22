# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'immobilierploermel_nestenn_com'
    execution_type='testing'
    country='france'
    locale='fr'
    source_name = "Immobilierploermel_PySpider_france_fr"
    landlord_n = "PLOERMEL NESTENN-IMMOBILIER"
    landlord_p = "02 97 93 03 00"
    landlord_e = "ploermel@nestenn.com"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://immobilier-ploermel.nestenn.com/?agence_name_hidden=1603875346&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"},
            {"url": "https://immobilier-ploermel.nestenn.com/?agence_name_hidden=1603875432&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Maison&type=Maison&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "house"},
            {"url": "https://immobilier-ploermel.nestenn.com/?agence_name_hidden=1603875390&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Studio&type=Studio&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"}
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='bienParent1']//a/@href[.!='/?action=compte/compte_accueil']").extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status=response.xpath("//img//@src[contains(.,'Loue')]").get()
        if status:
            return

        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//div[@id='annonce_entete_left']/h1/text()[contains(.,'Appartement')]").get()
        if prop_type: 
            property_type = "apartment"
        item_loader.add_value("property_type", property_type)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.source_name, input_type="VALUE")
        
        address = response.xpath("//div[@id='fil_ariane']/div[contains(.,'Location ')]/a/text()").get()
        if address: item_loader.add_value("address", " ".join(address.strip().split(" ")[2:4]))

        room_count = response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'piece')]/text()").get()
        if room_count: 
                item_loader.add_value("room_count", room_count.split("p")[0])
        else:       
            room_count = response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'chambre')]/text()").get()
            if room_count: 
                item_loader.add_value("room_count", room_count.split("c")[0])


        landlord_phone = response.xpath("//div[contains(@id,'telephone_bien')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        elevator = response.xpath("//img[contains(@src,'ascenseur')]//following-sibling::div//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//script[contains(.,'postalCode')]/text()", input_type="F_XPATH", split_list={'"postalCode": "': -1, '"': 0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@id='fil_ariane']/div[contains(.,'Location ')]/a/text()", input_type="F_XPATH", split_list={" ":2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@id='annonce_entete_left']/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='container']/p[1]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@id='groupeIcon']/div/div[contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='groupeIcon']/div/div[contains(.,'salle d eau')]/text() | //div[@id='groupeIcon']/div/div[contains(.,'salle de bain')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='content_prix']/text()[not(contains(.,'Loué') or contains(.,'Vendu'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie') and contains(.,'€')]", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slider_bien']/a/div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='groupeIcon']/div/div[contains(.,'garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value=self.landlord_n, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value=self.landlord_e, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@id='ref']/text()", input_type="F_XPATH", split_list={"Réf :":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@id='groupeIcon']/div/div[contains(.,'piscine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(@class,'consoEner')]/@style//parent::div/text()", input_type="F_XPATH")

        yield item_loader.load_item()