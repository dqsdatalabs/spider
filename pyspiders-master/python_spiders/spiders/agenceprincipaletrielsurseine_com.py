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
import re


class MySpider(Spider):
    name = 'agenceprincipaletrielsurseine_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agenceprincipaletrielsurseine.com/catalog/advanced_search_result.php?C_34_MAX=&C_41_search=EGAL&C_41_type=FLAG&C_41=&C_41_temp=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MIN=&C_36_MAX=&C_38_search=EGAL&C_38_type=NUMBER&C_38=&C_38_tmp=&action=update_search&search_id=&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=CONTIENT&C_33_type=TEXT&C_33=&C_34_search=COMPRIS&C_34_type=NUMBER&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MIN=&C_34_MIN=&C_30_MAX=", "property_type": "house"},
            {"url": "https://www.agenceprincipaletrielsurseine.com/catalog/advanced_search_result.php?action=update_search&search_id=1681780571332385&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_33_search=COMPRIS&C_33_type=NUMBER&C_33=&C_33_MAX=&C_34_search=COMPRIS&C_34_type=NUMBER&C_28_tmp=Location&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_34_MIN=&C_34_MAX=&C_30_MIN=&C_30_MAX=&C_33_MIN=", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']/div[contains(@id,'product_')]//div[@class='listing-cell']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "agenceprincipaletrielsurseine_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//div[@class='prix loyer']/span[@class='alur_loyer_price']/text()").extract_first()
        if price:
            price = price.split("€")[0].strip().split(" ")[-1].replace(" ","").replace("\xa0","")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency","EUR")
        
        external_id = response.xpath("//div[@class='prix loyer']/following-sibling::text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
 
        item_loader.add_xpath("floor", "//div[@class='panel-body']//div[contains(.,'Etage') and not(contains(.,'Dernier Etage'))]/following-sibling::div//text()")
        item_loader.add_xpath("zipcode","//div[@class='panel-body']//div[contains(.,'Code postal')]/following-sibling::div//text()")
 
        item_loader.add_xpath("city", "//div[@class='panel-body']//div[contains(.,'Ville')]/following-sibling::div//text()")
        item_loader.add_xpath("address","//div[@class='panel-body']//div[contains(.,'Ville')]/following-sibling::div//text()")
        item_loader.add_xpath("room_count","//div[@class='panel-body']//div[contains(.,'pièces')]/following-sibling::div//text()")
        item_loader.add_xpath("bathroom_count","//div[@class='panel-body']//div[contains(.,'de bains') or contains(.,'Salle(s) d')]/following-sibling::div//text()")

        square = response.xpath("//div[@class='panel-body']//div[contains(.,'Surface')]/following-sibling::div//text()").extract_first()
        if square:
            square_meters=square.split("m")[0]
            square_meters = math.ceil(float(square_meters.strip()))
            item_loader.add_value("square_meters",square_meters )
     
        desc = "".join(response.xpath("//div[@class='product-desc']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())

        utilities = response.xpath("//div[@class='panel-body']//div[contains(.,'Honoraires Locataire')]/following-sibling::div//text()[.!='Non communiqué']").extract_first()
        if utilities:
            utilities = math.ceil(float(utilities.split("EUR")[0].strip()))
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//div[@class='panel-body']//div[contains(.,'Dépôt de Garantie')]/following-sibling::div//text()").extract_first()
        if deposit:
            deposit = math.ceil(float(deposit.split("EUR")[0].strip()))
            item_loader.add_value("deposit", deposit)
        
        energy_label = response.xpath("//div[@class='panel-body']//div[contains(.,'Conso Energ')]/following-sibling::div//text()[.!='Non communiqué']").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        parking = response.xpath("//div[@class='panel-body']//div[contains(.,'parking')]/following-sibling::div//text()").extract_first()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        elevator = response.xpath("//div[@class='panel-body']//div[contains(.,'Ascenseur')]/following-sibling::div//text()").extract_first()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='panel-body']//div[contains(.,'balcon')]/following-sibling::div//text()").extract_first()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
   
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_vignettes']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01 39 70 77 77")
        item_loader.add_value("landlord_name", "Agence Principale Triel / Verneuil sur Seine")
        yield item_loader.load_item()