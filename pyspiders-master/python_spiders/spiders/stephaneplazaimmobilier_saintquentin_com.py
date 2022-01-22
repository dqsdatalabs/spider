# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import requests


class MySpider(Spider):
    name = 'stephaneplazaimmobilier_saintquentin_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://saintquentin.stephaneplazaimmobilier.com/location/appartement", "property_type": "apartment"},
	        {"url": "https://saintquentin.stephaneplazaimmobilier.com/location/maison", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response): 



        for item in response.xpath("//div[@class='room']/a/@href").getall():
            follow_url = item
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
 

                
        next_button=response.xpath("//ul[@class='pagination']//li[@class='page-item']/a/@href").get()
        if next_button:
            yield Request(next_button, callback=self.parse, meta={"property_type":response.meta["property_type"]})
 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@class='slide-top-left']/h1/text()").getall()
        if title:
            result = ""
            part1 = title[0].strip()
            part2 = title[1].replace(" ","").replace("\n"," ")
            result = part1 + part2
            item_loader.add_value("title", result)
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")

        images_product_id = response.xpath("//app-product-carousel/@productid").get()
        data_json = requests.get(f"https://saintquentin.stephaneplazaimmobilier.com/product/media/{images_product_id}")
        data_images = json.loads(data_json.content)

        if data_images:
            result = []
            for item in data_images["base"]:
                result.append(item["fullurl"])

            item_loader.add_value("images",result)
            item_loader.add_value("external_images_count",len(result))

        item_loader.add_value("external_source", "Stephaneplazaimmobilier_Saintquentin_PySpider_france")
        
        ext_id =response.xpath("//ul/li[contains(.,'Référence')]//text()").extract_first()
        if ext_id:
            item_loader.add_value("external_id",ext_id.replace("Référence","").strip() )    
        
        address =response.xpath("//div[label[contains(.,'Ville')]]/span//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() )    
            item_loader.add_value("city",address.strip() )    
                
        rent =response.xpath("//div[@class='roominfo']//text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
      
        floor = response.xpath("//div[label[contains(.,'Nombre étages')]]/span//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())      
        
        room_count = response.xpath("//div[label[.='Chambres']]/span//text()").extract_first() 
        room = response.xpath("//div[label[contains(.,'pièce')]]/span//text()").extract_first() 
        if room:
            item_loader.add_value("room_count",room)
        else:
            item_loader.add_value("room_count",room_count)

        
        available_date = response.xpath("//div[label[contains(.,'Date de disponibilité')]]/span//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        zipcode = response.xpath("//div[label[contains(.,'Code postal')]]/span//text()").extract_first() 
        if zipcode:   
            item_loader.add_value("zipcode",zipcode.strip())

        bathroom_count = response.xpath("//div[label[contains(.,'Salle d')]]/span//text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        square =response.xpath("//div[label[.='Surface']]/span//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 
        
        utilities =response.xpath("//div[label[contains(.,'Honoraires état des lieux locataire')]]/span//text()").extract_first()    
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 

        furnished =response.xpath("//div[label[contains(.,'Meublé')]]/span//text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//div[label[contains(.,'Ascenseur')]]/span//text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking =" ".join(response.xpath("//div[label[contains(.,'parking') or contains(.,'garage')]]/span//text()").extract())
        if parking:
            if "non" in parking.lower() or "1" not in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony =response.xpath("//div[label[contains(.,'balcon')]]/span//text()").extract_first()    
        if balcony:
            if "non" in balcony.lower()  or "0" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        terrace =response.xpath("//div[label[contains(.,'terrasse')]]/span//text()").extract_first()    
        if terrace:
            if "non" in terrace.lower()  or "0" in terrace:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        deposit =response.xpath("//div[label[contains(.,'de garantie')]]/span//text()").extract_first()    
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","")) 
        desc = " ".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        address = response.xpath("//div[@id='description']/div/span/text()").get()
        if address:
            item_loader.add_value("city",address)
            item_loader.add_value("address",address)
   
        item_loader.add_xpath("landlord_name", "//div[h3[contains(.,'agence')]]//div[@class='member-blk-contact']//div[@class='mtitle']//text()")
        item_loader.add_xpath("landlord_phone", "//div[h3[contains(.,'agence')]]//div[@class='member-contact']//a[contains(@href,'tel')]//@title")
        
        energy = response.xpath("//div[label[contains(.,'Conso Energ')]]/span//text()[not(contains(.,'Vierge'))]").extract_first() 
        if energy:   
            item_loader.add_value("energy_label",energy.strip())
        
        swimming_pool =response.xpath("//div[label[contains(.,'Piscine')]]/span//text()").extract_first()    
        if swimming_pool:
            if "non" in swimming_pool.lower() or "0" in swimming_pool:
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_phone","0323626219")
        item_loader.add_value("landlord_name","Anthony De sousa costa")
        item_loader.add_value("landlord_email","adesousacosta@stephaneplazaimmobilier.com")

        yield item_loader.load_item()