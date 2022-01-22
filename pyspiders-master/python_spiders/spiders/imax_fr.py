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
    name = 'imax_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source= 'Imax_fr_PySpider_france'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.imax.fr/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=0&C_33_MAX=&C_30_MIN=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=", "property_type": "apartment"},
            {"url": "https://www.imax.fr/catalog/advanced_search_result.php?action=update_search&search_id=1708890794974020&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=0&C_33_MAX=&C_30_MIN=&C_34_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=", "property_type": "house"},   
            #{"url": "https://www.imax.fr/catalog/advanced_search_result.php?action=update_search&search_id=1708864536175802&C_28=Location&C_28_search=EGAL&C_28_type=UNIQUE&C_27_search=EGAL&C_27_type=TEXT&C_27=Loft&C_27_tmp=Loft&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=0&C_33_MAX=&C_30_MIN=&C_34_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//a[@class='listing__link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        next_page = response.xpath("//li[@class='active']/following-sibling::li[1]/a//@href").get()
        if next_page:
            url = response.urljoin(next_page)
            yield Request(url, callback=self.parse, meta={"property_type":response.meta["property_type"]})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Imax_fr_PySpider_france")
        item_loader.add_value("external_link", response.url)
        #item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("property_type", response.meta["property_type"])
        external_id = response.xpath("//div[@class='product-reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        title=response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title",title.split("*")[0])

        rent = response.xpath("//div[@class='row']/div[contains(.,'Loyer charges')]/following-sibling::div/b/text()").get()
        if rent:
            if "." in rent:
                rent=rent.split("EUR")[0].split(".")[0]
                item_loader.add_value("rent", rent) 
            else:
                rent=rent.split("EUR")[0]
                item_loader.add_value("rent", rent)  

        utilities = response.xpath("//div[@class='row']/div[contains(.,'Provision')]/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("EUR")[0].split(".")[0])
        deposit = response.xpath("//div[@class='row']/div[contains(.,'Garantie')]/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0].split(".")[0])
        item_loader.add_value("currency", 'EUR')
        
        city = response.xpath("//div[@class='product-aside-city']/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("(//span[@class='alur_location_ville']/text())[1]").get()
        if zipcode:
            zipcode = zipcode.split(" ")[0].strip().split(" ")[0].strip()
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("(//span[@class='alur_location_ville']/text())[1]").get()
        if address:
            item_loader.add_value("address", address)
        
        room_count = response.xpath("//div[@class='row']/div[contains(.,'Nombre pi')]/following-sibling::div/b/text()").get()
        if room_count:           
            item_loader.add_value("room_count", room_count)
        bathroom = response.xpath("//div[@class='row']/div[contains(.,'bain')]/following-sibling::div/b/text()").get()
        if bathroom:    
            item_loader.add_value("bathroom_count", bathroom)
        
        energy_label = response.xpath("//div[@class='row']/div[contains(.,'Consommation')]/following-sibling::div/b/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        available_date = response.xpath("//div[@class='row']//div[contains(.,'Disponibilité')]//b//text()[contains(.,'/')]").get()
        if available_date:
            item_loader.add_value("available_date", available_date)

        square = response.xpath("(//div[@class='row']/div[contains(.,'Surface')]/following-sibling::div/b/text())[1]").extract_first()
        if square:
            square =square.split("m")[0].strip()
            square_meters = math.ceil(float(square.strip()))
            item_loader.add_value("square_meters",square_meters )

        furnished = "".join(response.xpath("(//div[@class='row']/div[contains(.,'Meublé')]/following-sibling::div/b/text())[1]").getall())
        if furnished:
            if "oui" in furnished.lower() :
                item_loader.add_value("furnished",True)

        desc = "".join(response.xpath("//div[@class='product-description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "ascenseur" in desc:
                item_loader.add_value("elevator", True)
            if "terrasse" in desc:
                item_loader.add_value("terrace", True)
            if "meublé" in desc:
                item_loader.add_value("furnished", True)

        images = [x for x in response.xpath("//div[@id='slider_product_large']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        item_loader.add_value("landlord_name", "IMAX")

        landlord_phone = "".join(response.xpath("//div[@class='product-agence-contact']//a[contains(@class,'btn-phone')]//@href").get())
        if landlord_phone:
                item_loader.add_value("landlord_phone",landlord_phone.split(":")[1])


        yield item_loader.load_item()