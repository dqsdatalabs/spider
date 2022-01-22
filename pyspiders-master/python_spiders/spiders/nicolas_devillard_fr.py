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
class MySpider(Spider):
    name = 'nicolas_devillard_fr'  
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    start_urls = ["https://www.nicolas-devillard.fr/category/location/"] #LEVEL-1
    external_source="Nicolas_Devillard_PySpider_france"

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'elementor-button-link')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
       
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented =response.xpath("//div[contains(@class,'dispo')]//text()[contains(.,'est louée')]").extract_first()
        if rented:                
            return

        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[contains(@class,'elementor-widget-container')]/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else: 
            return
 
        item_loader.add_value("external_source", self.external_source)

        title =" ".join(response.xpath("//div//h1[contains(@class,'elementor-heading-title')]//text()").extract())
        if title:
            item_loader.add_value("title",title.strip() )   
        
        address = response.xpath("//div[contains(@class,'elementor-element-41d7ebb')]//h1//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            zipcode = address.split(" ")[0]
            city = address.replace(zipcode,"")   
            item_loader.add_value("zipcode",zipcode.strip() )    
            item_loader.add_value("city",city.strip() ) 

        rent =response.xpath("//div[contains(@class,'elementor-element-00183d4')]//div//text()[contains(.,'€')][normalize-space()]").extract_first()
        if rent:                
            item_loader.add_value("rent_string",rent)  

        utilities =response.xpath("//div[contains(@class,'elementor-element-57482574')]//p//text()[contains(.,' provision')]").extract_first()
        if utilities:     
            item_loader.add_value("utilities", utilities.split(" provision")[1].split("€")[0].strip()) 

        deposit =response.xpath("//div[contains(@class,'elementor-element-57482574')]//p//text()[contains(.,'Dépôt de garantie')]").extract_first()
        if deposit:     
            item_loader.add_value("deposit", deposit)  
            
        available_date = response.xpath("//div[contains(@class,'elementor-element-3208de43')]//p//text()[contains(.,'Disponible le')]").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Disponible le","").strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        if "studio" in  get_p_type_string(f_text):
            item_loader.add_value("room_count","1") 
        else:
            room =response.xpath("//div[@data-id='1164873c']/div[@class='elementor-widget-container']/text()").get()
            if room:
                room_count = room.replace("\n","").replace("\t","").strip()
                if "+" in room_count:
                    room_count1 = room_count.split("+")[0]
                    room_count2 = room_count.split("+")[1]
                    room_count=int(room_count1)+int(room_count2)
                    item_loader.add_value("room_count",room_count)
                elif "/" in room_count:
                    room_count = room_count.split("/")[0]
                    item_loader.add_value("room_count",room_count)
                elif "–" in room_count:
                    room_count = room_count.split("–")[0].strip()
                    item_loader.add_value("room_count",room_count)
                elif " ou" in room_count:
                    room_count = room_count.split(" ou")[0]
                    item_loader.add_value("room_count",room_count)
                else:
                    item_loader.add_value("room_count",room_count.strip()) 
        

        bathroom_count =response.xpath("//span[contains(.,'salle(s) de bain')]//parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.replace("\n","").replace("\t","")) 

        square = " ".join(response.xpath("//div/div[@class='elementor-widget-container']//div[contains(.,'M2')]//text()").extract())
        if square:
            square_meters =  square.split("M")[0].strip()
            item_loader.add_value("square_meters",square_meters) 

        energy_label =response.xpath("//div[@data-id='66f88424']//text()[normalize-space()][.!='NC']").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  
     
        terrace =response.xpath("//div[@class='elementor-widget-container']/div[contains(.,'Terrasse')]/text()[normalize-space()]").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        balcony =response.xpath("//div[@class='elementor-widget-container']/div[contains(.,'Balcon')]/text()[normalize-space()]").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        elevator =response.xpath("//div/div[@class='elementor-widget-container']//div[contains(.,'Ascenseur')]//text()").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)
        parking =response.xpath("//div[@class='elementor-widget-container']//div[contains(.,'parking') or contains(.,'Parking')]/text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        furnished =response.xpath("//div/div[@class='elementor-widget-container']//span[contains(.,'meublée')]//text()").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)

        floor =response.xpath("//div[contains(@class,'elementor-widget-text-editor')][div[contains(.,'étage')]]/preceding-sibling::div[1]//text()[normalize-space()]").extract_first()    
        if floor:
            item_loader.add_value("floor", floor.strip())

        desc = " ".join(response.xpath("//div[contains(@class,'elementor-element-3208de43')]//div[@class='elementor-widget-container']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='ee-slider__slide ee-swiper__slide swiper-slide']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        item_loader.add_xpath("latitude", "//div[@class='ee-google-map']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='ee-google-map']/@data-lng")

        item_loader.add_xpath("landlord_name", "//div[@data-id='e7f651e']//b/text()")
        item_loader.add_xpath("landlord_phone", "//div[@data-id='e7f651e']//div/text()[normalize-space()]")
        item_loader.add_xpath("landlord_email", "//div[@data-id='e7f651e']//div/span//text()[normalize-space()] | //div[@data-id='e7f651e']//div/a//text()[normalize-space()]")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("pièces" in p_type_string.lower() or "pieces" in p_type_string.lower()):
        return "room"
    else:
        return None