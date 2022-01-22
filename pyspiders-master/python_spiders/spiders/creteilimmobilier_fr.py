# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math


class MySpider(Spider):
    name = 'creteilimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://creteil-immobilier.fr/property_type/location/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'ecs-posts')]/article"):
            follow_url = response.urljoin(item.xpath(".//p/a/@href").get())
            prop_type = item.xpath(".//p/a/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            elif "duplex" in prop_type.lower():
                property_type = "apartment"
            elif "villa" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_xpath("title", "normalize-space(//div[contains(@class,'elementor-widget-ae-post-title')]//h1/text())")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
       
        item_loader.add_value("external_source", "Creteilimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("external_id", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Référence')]/following-sibling::div//text()")

        item_loader.add_xpath("rent_string", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Loyer')]/following-sibling::div//text()")
        item_loader.add_xpath("floor", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Etage')]/following-sibling::div//text()")
        item_loader.add_xpath("address", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Ville')]/following-sibling::div//text()")
        item_loader.add_xpath("city", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Ville')]/following-sibling::div//text()")
        item_loader.add_xpath("room_count", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'pièce')]/following-sibling::div//text()")
        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Salle(s) de bain')]/following-sibling::div//text()[.!='0']")
        item_loader.add_xpath("zipcode", "//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Code Postal')]/following-sibling::div//text()")
        
        square = response.xpath("//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Surface Habitable')]/following-sibling::div//text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.split("m")[0]))
            item_loader.add_value("square_meters", str(square_meters).strip())
          
        deposit = response.xpath("//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Dépôt de Garantie')]/following-sibling::div//text()").extract_first()
        if deposit :
            item_loader.add_value("deposit",deposit.split("€")[0].strip() )

        utilities = response.xpath("//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Charges') and not(contains(.,'Loyer'))]/following-sibling::div//text()").extract_first()
        if utilities :
            item_loader.add_value("utilities",utilities.split("€")[0].strip() )
               
        desc = "".join(response.xpath("//div[@class='ae-element-post-content']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
  
        energy = response.xpath("//div[@class='diagnostic-scale']//span/text()[normalize-space()]").extract_first()
        if energy:
            label = int(float(energy))
            item_loader.add_value("energy_label", energy_label_calculate(label))
       
        furnished = response.xpath("//div[contains(@class,'elementor-column-gap-no')]//div[contains(.,'Meublé')]/following-sibling::div//text()").extract_first()
        if furnished:
            if "NON" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
       
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-slide']//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01 45 13 10 10")
        item_loader.add_value("landlord_email", "cvi@immo-creteil.fr")
        item_loader.add_value("landlord_name", "Créteil Vajou Immobilier")

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label