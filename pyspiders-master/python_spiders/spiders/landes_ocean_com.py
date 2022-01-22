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
from html.parser import HTMLParser
import math
class MySpider(Spider):
    name = 'landes_ocean_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.landes-ocean-moliets.com/catalog/advanced_search_result.php?action=update_search&search_id=1680987686366904&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.landes-ocean-moliets.com/catalog/advanced_search_result.php?action=update_search&search_id=1680987686366904&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C3%2C17&C_27_tmp=2%2C3%2C17&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&keywords=",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='link_img']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Landes_ocean_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div[contains(@class,'content_intro_description')]//text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//div[@id='content_details']//li[contains(.,'Référence')]//text()").extract_first()
        if external_id :
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
            
        room_count = response.xpath("//div[@id='content_details']//li[contains(.,'Chambres')]//text()").extract_first()
        if room_count :
            item_loader.add_value("room_count", room_count.split(":")[1].strip())

        deposit = response.xpath("substring-after(//div[@class='col-sm-12 content_details_description']/p[contains(.,'Dépôt')],'Dépôt de garantie : ')").extract_first()
        if deposit :
            item_loader.add_value("deposit", deposit.split("€")[0].strip())


        bathroom_count = response.xpath("//li[@class='list-group-item odd']/div[contains(.,'Salle(s) de bains')]/div[2]//text()").extract_first()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        square_meters = response.xpath("//div[@id='content_details']//li[contains(.,'Surface habitable')]//text()").extract_first()
        if square_meters :           
            square_meters = math.ceil(float(square_meters.split(":")[1].replace("m²","").strip()))
            item_loader.add_value("square_meters", str(square_meters))
        else: return
        
        price = response.xpath("//span[@class='alur_loyer_price']//text()").extract_first()
        if price :
            if "/" in price:
                price = price.split("/")[0]
                if "Loyer " in price:
                    price = price.split("Loyer ")[1]
                    item_loader.add_value("rent_string", price)                  
                else:
                    item_loader.add_value("rent_string", price)
        else: return
        
        desc = "".join(response.xpath("//div[contains(@class,'content_details_description')]//text()[.!='Nos honoraires']").extract())
        if desc :
            item_loader.add_value("description", desc.strip())

        utilities = "".join(response.xpath("//ul/li/div/div[contains(.,'charges')]/following-sibling::div//text()").extract())
        if utilities :
            item_loader.add_value("utilities", utilities.replace("EUR","").strip())

        item_loader.add_xpath("city", "//div[@id='product_criteres']//div[@class='row'][contains(.,'Ville')]/div[2]//text()")
        item_loader.add_xpath("address", "//div[@id='product_criteres']//div[@class='row'][contains(.,'Ville')]/div[2]//text()")
        
        elevator = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'Ascenseur')]/div[2]//text()").extract_first()
        if elevator :
            if elevator == "Non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        swimming_pool = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'Piscine')]/div[2]//text()").extract_first()
        if swimming_pool :
            if swimming_pool == "Non":
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)

        parking = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'parking')]/div[2]//text()").extract_first()
        if parking :
            if parking == "Non":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'terrasses')]/div[2]//text()").extract_first()
        if terrace :
            if terrace == "Non":
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
       
        zipcode = response.xpath("//span[@class='alur_location_ville']//text()").extract_first()
        if zipcode:
            zipcode = zipcode.split(" ")[0].strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

        energy_label = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'Conso Energ')]/div[2]//text()[.!='Vierge' and .!='Non communiqué']").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'Meublé')]/div[2]//text()").extract_first()
        if furnished :
            if furnished == "Non":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//a/@href[.!='#']").extract()]
        
        if images :
                item_loader.add_value("images", images)
        
        floor = response.xpath("//div[@id='product_criteres']//div[@class='row'][contains(.,'Etage' ) or contains(.,'Nombre étages' ) ]/div[2]//text()").extract_first()
        if floor :  
           item_loader.add_value("floor", floor)

        item_loader.add_value("landlord_name", "ERA LANDES OCEAN")
        item_loader.add_value("landlord_phone", "05.58.48.50.29")
               
        yield item_loader.load_item()

        
       

        
        
          

        

      
     