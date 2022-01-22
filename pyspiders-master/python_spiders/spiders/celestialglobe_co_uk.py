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
    name = 'celestialglobe_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["https://www.lscg.co.uk/letting/search/1?location=3&ptypes=5&range_low=0&range_high=5000&size_low=0&size_high=&bed_low=0&bed_high=&en_keywords=&sort="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        for item in data["house"]:
            info = item["info"]
            follow_url = f"https://www.lscg.co.uk/letting/property/{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"info":info})
        
        if page <= data["total_page"]:
            p_url = f"https://www.lscg.co.uk/letting/search/{page}?"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        f_text = response.meta["info"]
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Celestialglobe_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div[@class='details-panel']/h1//text()")        
                      
        address =response.xpath("//div[@class='details-panel']/h3//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            city = address.split(",")[0].strip()
            item_loader.add_value("city",city.strip() ) 

        rent = " ".join(response.xpath("//div[@class='price']//text()[normalize-space()]").extract())
        if rent:
            if "week" in rent.lower():
                rent = rent.split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))                
            else:
                rent = rent.split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'GBP')
        item_loader.add_xpath("zipcode", "//div[@id='info']//p[contains(.,'Postcode')]/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='size-info']//div[i[contains(.,'Bath')]]/text()[normalize-space()]")
        item_loader.add_xpath("external_id","substring-after(//div[@class='ref-num']/text()[normalize-space()],'Ref. ')")                
        
        available_date = response.xpath("//div[@id='info']//p[contains(.,'Available From')]/text()[.!='Unknow']").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.replace("Available from","").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room_count = response.xpath("//div[@class='size-info']//div[i[contains(.,'Beds')]]/text()[normalize-space()]").extract_first() 
        if room_count and "Studio" in room_count:
            item_loader.add_value("room_count","1") 
        elif room_count:   
            item_loader.add_value("room_count",room_count.strip())          
              
        square =response.xpath("//div[@class='size-info']//div[i[contains(.,'m²')]]/text()[normalize-space()]").extract_first()
        if square and square.strip()!="0":
            item_loader.add_value("square_meters", square) 
       
        furnished =response.xpath("//div[@id='info']//div[p[contains(.,'Furniture')]]/i/@class").extract_first()    
        if furnished:
            if "active" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        balcony =response.xpath("//div[@id='info']//div[p[contains(.,'Balcony')]]/i/@class").extract_first()    
        if balcony:
            if "active" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        parking =response.xpath("//div[@id='info']//div[p[contains(.,'Parking Lot')]]/i/@class").extract_first()    
        if parking:
            if "active" in parking.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        swimming_pool =response.xpath("//div[@id='info']//div[p[contains(.,'Swimming Pool')]]/i/@class").extract_first()    
        if parking:
            if "active" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
            else:
                item_loader.add_value("swimming_pool", False)        
            
        desc = " ".join(response.xpath("//div[@class='dp-description__text']//text() | //div[@id='info']/div[@class='pure-g']/div[@class='pure-u-1']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        img=response.xpath("//div[@class='swiper-wrapper']/div[@class='swiper-slide']/@style").extract() 
        if img:
            images=[]
            for x in img:
                image = x.split("background-image:url(")[1].split(")")[0]
                images.append(response.urljoin(image))
            if images:
                item_loader.add_value("images",  list(set(images)))

        item_loader.add_value("landlord_name", "Celestial Globe")
        item_loader.add_value("landlord_phone", "+44 (0) 203 9681888")
        item_loader.add_value("landlord_email", "uk@celestialglobe.co.uk")    
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None