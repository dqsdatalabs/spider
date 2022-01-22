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
import re

class MySpider(Spider):
    name = 'groupe123immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Groupe123immo_PySpider_france"
    def start_requests(self):
        yield Request(
                "https://www.groupe123immo.com/location/1",
                callback=self.parse,
            )


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item__data']/div//div//div[@class='links-group__wrapper']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("external_link", response.url)

        prop_type = response.xpath("//li[@class='breadcrumb__item']/a/text()[contains(.,'Appartement')]").get()
        if prop_type:
            property_type = "apartment"
        else:
            prop_type = response.xpath("//li[@class='breadcrumb__item']/a/text()[contains(.,'Maison')]").get()
            if prop_type:
                property_type = "house"
        if property_type:
            item_loader.add_value("property_type", property_type)

        external_id=response.xpath("//div[@class='property-detail-v3__info-id']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())

        title = "".join(response.xpath("//div[@class='title-subtitle']/h1/span//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent=response.xpath("//div[@class='main-info__price price']/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[.='Ville']/following-sibling::span/text()", input_type="M_XPATH")
        city = response.xpath("//span[@class='table-aria__td table-aria__td--title'][contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode=response.xpath("//span[.='Code postal']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        # # ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/span[contains(.,'Ville')]/following-sibling::span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[.='Code']/following-sibling::span/text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p/span[contains(.,'Etage')]/following-sibling::span/text()", input_type="M_XPATH")
        
        square_meters = response.xpath("//span[contains(.,'Surface habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'pièces')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'salle')]/following-sibling::span/text()", input_type="M_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[@class='about__text-block text-block']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        images=[response.urljoin(x) for x in response.xpath("//picture/parent::a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
            
        # ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p/span[contains(.,'parking')]/following-sibling::span/text()[.!='0']", input_type="M_XPATH", tf_item=True)
        
        furnished = response.xpath("//span[.='Meublé']/following-sibling::span/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//span[.='Ascenseur']/following-sibling::span/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False) 
        
        utilities = response.xpath("//span[@class='table-aria__td table-aria__td--title'][contains(.,'Charges locatives')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())

        deposit = response.xpath("//span[@class='table-aria__td table-aria__td--title'][contains(.,'Dépôt')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        # ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat :":1, ",":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        
        item_loader.add_value("landlord_name", "GROUPE 123 IMMO")
        landlord_phone = response.xpath("//div[@class='coords__element coords-phone']/a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", "auxerre@groupe123immo.com")


        yield item_loader.load_item()
