# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'immobilier_allauch_nestenn_com'
    b_url = "https://immobilier-allauch.nestenn.com"
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immobilier_Allauch_Nestenn_PySpider_france"
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
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        address = response.xpath("//div[@id='fil_ariane']/div[contains(.,'Location ')][last()]/a/text()").get()
        if address: 
            item_loader.add_value("address", " ".join(address.strip().split(")")[0].split(" ")[2:])+")")
            item_loader.add_value("city", " ".join(address.strip().split("(")[0].split(" ")[2:]).strip())
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//script[contains(.,'postalCode')]/text()", input_type="F_XPATH", split_list={'"postalCode": "': -1, '"': 0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[@id='annonce_entete_left']/h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='container']/p[1]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@id='groupeIcon']/div/div[contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@id='groupeIcon']/div/div[contains(.,'chambre')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@id='groupeIcon']/div/div[contains(.,'salle d eau') or contains(.,'salle de bain')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='content_prix']/text()[not(contains(.,'Loué') or contains(.,'Vendu'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie') and contains(.,'€')]", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slider_bien']/a/div/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lngLat')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='groupeIcon']/div/div[contains(.,'garage') or contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Nestenn immobilier Allauch", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 91 08 32 35", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="allauch@nestenn.com", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@id='ref']/text()", input_type="F_XPATH", split_list={"Réf :":1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='groupeIcon']/div/div[contains(.,'meuble')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@id='groupeIcon']/div/div[contains(.,'piscine')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='dpeBox']//img[contains(@alt,'DPE_')]/@alt[not(contains(.,'NC'))]", input_type="F_XPATH", split_list={"_":1})

        yield item_loader.load_item()