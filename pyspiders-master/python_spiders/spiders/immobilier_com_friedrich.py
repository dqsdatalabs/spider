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
import re

class MySpider(Spider):
    name = 'immobilier_com_friedrich'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.friedrich-immobilier.com/catalog/advanced_search_result_carto.php?action=update_search&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27=1%2C&C_27_search=EGAL&C_27_type=UNIQUE&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_30=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MIN=&page=1&search_id=1681045942257302&nb_rows_per_page=100",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.friedrich-immobilier.com/catalog/advanced_search_result_carto.php?action=update_search&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27=2%2C&C_27_search=EGAL&C_27_type=UNIQUE&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_30=0&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_34_MAX=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MIN=&page=1&search_id=1681045918519396&nb_rows_per_page=100",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='link_detail']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        

        next_page = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Immobilierfriedrich_PySpider_"+ self.country + "_" + self.locale)
  
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        room_count=response.xpath("//div[@class='item']/span[contains(.,'chambres')]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        bathroom_count=response.xpath("//div[@class='item']/span[contains(.,'salle')]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        utilities=response.xpath("//div[contains(@class,'formatted_price')]/span[contains(.,'Honora')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        square_mt=response.xpath("//div[@class='item']/span[contains(.,'habitable')]/text()").extract_first()
        if square_mt:
            item_loader.add_value("square_meters", square_mt.strip().split(" ")[0].split("m")[0])
       
        
        address="".join(response.xpath("//div[@class='item'][1]/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())    
            item_loader.add_value("city", address.strip())    
        zipcode=response.xpath("//span[@class='alur_location_ville']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[0])
      
        
        rent=response.xpath("//div[@class='price_container']/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        external_id=response.xpath("//span[@itemprop='name'][contains(.,'Ref')]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
            
        images=[x for x in response.xpath("//ul[@class='slides']/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        desc="".join(response.xpath("//p[@itemprop='description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc)
            
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","").replace(":","").strip()
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        washing_machine = response.xpath("//p[@itemprop='description']/text()[contains(.,'machine à laver')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        deposit="".join(response.xpath("//div[@class='formatted_price_alur2_div'][contains(.,'garantie:')]//text()").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("garantie:")[1].strip().split(" ")[0].split("€")[0])
            
        energy_label=response.xpath("//div[@class='dpe-letter']/span/text()").extract_first()
        if energy_label:
            if energy_label=='NI':
                pass
            elif energy_label=='NCI':
                pass
            else:
                item_loader.add_value("energy_label", energy_label)
            
        
        landlord_name=response.xpath("//div[@class='contact']/p/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        phone=response.xpath("//div[@class='contact']/a[1]/@data-content").extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone)
            
        email=response.xpath("//div[@class='contact']/a[2]/@data-content").extract_first()
        if email:
            item_loader.add_value("landlord_email", email)
        
        
        latitude_longitude=response.xpath("//script[contains(.,'myLatlng')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            lng=latitude_longitude.split('LatLng(')[1].split(',')[0].split(")")
            if lat or lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude", lng)
        
        yield item_loader.load_item()