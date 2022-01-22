# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
class MySpider(Spider):
    name = 'lauragais_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.lauragais-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1689472311962110&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.lauragais-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17%2C30&C_27_tmp=2&C_27_tmp=17&C_27_tmp=30&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='photo-product']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath("./div/span/text()").get()
            if status and "loué" in status.lower():
                continue
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//li[@class='next-link']/a/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Lauragais_Immo_PySpider_france")
        title =response.xpath("//div/h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())        
         
        address = response.xpath("//div[@class='container']//div[@class='product-localisation']/text()").extract_first()
        if address:
            item_loader.add_value("address",address ) 
            zipcode = address.strip().split(" ")[0]
            city = " ".join(address.strip().split(" ")[1:])
            item_loader.add_value("zipcode",zipcode)
            item_loader.add_value("city",city) 
   
        room_count = response.xpath("//div[@class='panel-body']//li/div[div[.='Chambres']]/div[2]//text()").extract_first()
        if not room_count:
            room_count = response.xpath("//div[@class='panel-body']//li/div[div[.='Nombre pièces']]/div[2]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count) 
        item_loader.add_xpath("external_id", "substring-after(//span[contains(.,'Ref')]/text(),': ')")
 
        item_loader.add_xpath("bathroom_count", "//div[@class='panel-body']//li/div[div[contains(.,'Salle(s) d')]]/div[2]//text()")
        rent =" ".join(response.xpath("//div[@class='container']//div[@class='product-price']//text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace('\xa0', '').replace(' ','').split("€")[0].split(".")[0])  
            item_loader.add_value("currency","EUR")  
  
        utilities =response.xpath("//div[@class='panel-body']//li/div[div[.='Provision sur charges']]/div[2]//text()").extract_first()
        if utilities:     
            utilities = utilities.split("EUR")[0].strip().replace('\xa0', '').replace(' ','')
            item_loader.add_value("utilities", int(float(utilities.replace(",","."))))  
   
        deposit =response.xpath("//div[@class='panel-body']//li/div[div[.='Dépôt de Garantie']]/div[2]//text()").extract_first()
        if deposit:   
            deposit = deposit.split("EUR")[0].strip().replace('\xa0', '').replace(' ','')
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
        
        date_parsed = ""
        available_date = response.xpath("//div[@class='panel-body']//li/div[div[.='Disponibilité']]/div[2]//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip())
        else:
            available_date = response.xpath("//div[@class='desc-text']/text()[contains(.,'Disponibilité :')]").extract_first() 
            if available_date:
                date_parsed = dateparser.parse(available_date.split("Disponibilité :")[1].split(".")[0].strip(), date_formats=["%d %B %Y"], languages=['fr'])
        if date_parsed:
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        square =response.xpath("//div[@class='panel-body']//li/div[div[.='Surface']]/div[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        energy_label =response.xpath("//div[@class='panel-body']//li/div[div[.='Conso Energ']]/div[2]//text()").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  
   
        item_loader.add_xpath("floor","//div[@class='panel-body']//li/div[div[.='Nombre étages']]/div[2]//text()")  
        parking =response.xpath("//div[@class='panel-body']//li/div[div[.='Type de Stationnement'] or contains(.,'places parking')  or contains(.,'garages/Box') ]/div[2]//text()").extract_first()    
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace =response.xpath("//div[@class='panel-body']//li/div[div[.='Nombre de terrasses']]/div[2]//text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        balcony =response.xpath("//div[@class='panel-body']//li/div[div[.='Nombre balcons']]/div[2]//text()").extract_first()    
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        swimming_pool =response.xpath("//div[@class='panel-body']//li/div[div[.='Piscine']]/div[2]//text()").extract_first()    
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
 
        elevator =response.xpath("//div[@class='panel-body']//li/div[div[.='Ascenseur']]/div[2]//text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        furnished =response.xpath("//div[@class='panel-body']//li/div[div[.='Meublé']]/div[2]//text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
    
        desc = " ".join(response.xpath("//div[@class='desc-text']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']/div/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        item_loader.add_xpath("landlord_name", "//div[@id='bloc_coord_agence']//div[@class='bloc-name']/text()")
        item_loader.add_xpath("landlord_phone", "substring-after(//div[@id='bloc_coord_agence']//div[@class='bloc-links']/a[contains(@href,'tel:')]/@href,'tel:')")
        
        yield item_loader.load_item()