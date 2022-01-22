# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobilier_montreuil_nestenn_com'
    execution_type='testing'
    country='france'
    locale='fr'
    # DYNAMIC DATA
    b_url = "https://immobilier-montreuil.nestenn.com"
    external_source = "Immobilier_Montreuil_Nestenn_PySpider_france"
    landlord_n = "Agent immobilier Montreuil"
    landlord_p = "01 43 63 21 27"
    # -------------
    
    def start_requests(self):
        start_urls = [
            {"url": f"{self.b_url}/?agence_name_hidden=1609416888&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=",
             "property_type": "apartment"},
            {"url": f"{self.b_url}/?agence_name_hidden=1609416892&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Maison&type=Maison&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=",
             "property_type": "house"},
            {"url": f"{self.b_url}/?agence_name_hidden=1609417413&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Studio&type=Studio&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=",
             "property_type": "studio"}
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
        
        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        prop_type = response.xpath("//div[@id='annonce_entete_left']/h1/text()[contains(.,'Appartement')]").get()
        if prop_type: 
            property_type = "apartment"
        item_loader.add_value("property_type", property_type)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        
        address = response.xpath("//div[@id='fil_ariane']/div[contains(.,'Location ')]/a/text()").get()
        if address: item_loader.add_value("address", " ".join(address.strip().split(" ")[2:4]))
        room_count = response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'chambre')]/text()").get()
        if room_count: 
            item_loader.add_value("room_count", room_count.split("c")[0])
        else:
            room_count = response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'piece')]/text()").get()
            if room_count: 
                item_loader.add_value("room_count", room_count.split("p")[0])
         
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//script[contains(.,'postalCode')]/text()", input_type="F_XPATH", split_list={'"postalCode": "': -1, '"': 0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@id='fil_ariane']/div[contains(.,'Location ')]/a/text()", input_type="F_XPATH", split_list={" ":2})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@id='annonce_entete_left']/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='container']/p[1]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@id='groupeIcon']/div/div[contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='groupeIcon']/div/div[contains(.,'salle d eau')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='content_prix']/text()[not(contains(.,'Loué') or contains(.,'Vendu'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie') and contains(.,'€')]", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slider_bien']/a/div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='groupeIcon']/div/div[contains(.,'garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value=self.landlord_n, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value=self.landlord_p, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="montreuil@nestenn.com", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@id='ref']/text()", input_type="F_XPATH", split_list={"Réf :":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@id='groupeIcon']/div/div[contains(.,'piscine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[contains(@class,'consoEner')]/@style//parent::div/text()", input_type="F_XPATH")

        yield item_loader.load_item()