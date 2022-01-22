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
    name = 'altijdwonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = [
        "https://www.altijdwonen.nl/huurwoning-utrecht.php",
        "https://www.altijdwonen.nl/huurwoning-amsterdam.php",
        "https://www.altijdwonen.nl/huurwoning-rotterdam.php",
        "https://www.altijdwonen.nl/huurwoning-den-haag.php",
        ]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='row']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        status=response.xpath("//td/strong[.='Status']/parent::td/following-sibling::td/text()").get()
        if status and "verhuurd" in status.lower():
            return 


        f_text = "".join(response.xpath("//td[contains(.,'Soort huurwoning')]/following-sibling::*/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[.='Omschrijving']/following-sibling::p/text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        item_loader.add_value("external_source", "Altijdwonen_PySpider_netherlands")          
        item_loader.add_xpath("external_id", "//tr[td[contains(.,'Woning ID')]]/td[2]/text()")          
        item_loader.add_xpath("deposit", "//tr[td[.='Borg']]/td[2]/text()[contains(.,'€')][.!='€ ']")          
        item_loader.add_xpath("utilities", "//tr[td[.='Servicekosten']]/td[2]/text()[contains(.,'€')][.!='€ ']")          
        item_loader.add_xpath("floor", "//tr[td[.='Verdieping']]/td[2]/text()[.!='Niet opgegeven' and .!='Gehele huurwoning']")          
        title = response.xpath("//div[@id='myTabContent']//div[@class='he2']/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )  
            if "(" in title:
                zipcode = title.split("(")[-1].split(")")[0]
                city = title.split("(")[0].strip().split(" ")[-1]
                item_loader.add_value("zipcode",zipcode.strip() ) 
                item_loader.add_value("city",city.strip() )    
        address = response.xpath("//ol/li[last()]/text()").extract_first()
        if address:            
            item_loader.add_value("address",address.strip() ) 

        room_count = response.xpath("//tr[td[contains(.,'Aantal kamer')]]/td[2]/text()[.!='Niet opgegeven' and .!='Gehele huurwoning']").extract_first() 
        if room_count: 
            if int(room_count)>=2:
               item_loader.add_value("room_count",int(room_count)-1) 
            else:
                item_loader.add_value("room_count",int(room_count)) 


        rent = response.xpath("//tr/td[.='Totale huurprijs']/following-sibling::td[1]//text()").extract_first() 
        if rent: 
            item_loader.add_value("rent_string",rent)      
        square =response.xpath("//tr[td[contains(.,'Oppervlakte')]]/td[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 
      
        available_date = response.xpath("//table//tr/td[strong[contains(.,'Beschikbaar per')]]/following-sibling::td/text()").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.replace("Per direct","now").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)     
  
        parking = response.xpath("//ul/li[.=' Garage']/i/@class").extract_first()    
        if parking:
            if "fa-times" in parking.lower().strip():
                item_loader.add_value("parking", False)
            elif "fa-check" in parking.lower().strip():
                item_loader.add_value("parking", True)
        balcony = response.xpath("//ul/li[.=' Balkon']/i/@class").extract_first()    
        if balcony:
            if "fa-times" in balcony.lower().strip():
                item_loader.add_value("balcony", False)
            elif "fa-check" in balcony.lower().strip():
                item_loader.add_value("balcony", True)
        terrace = response.xpath("//ul/li[.=' Terras']/i/@class").extract_first()    
        if terrace:
            if "fa-times" in terrace.lower().strip():
                item_loader.add_value("terrace", False)
            elif "fa-check" in terrace.lower().strip():
                item_loader.add_value("terrace", True)
        elevator = response.xpath("//ul/li[.=' Lift aanwezig']/i/@class").extract_first()    
        if elevator:
            if "fa-times" in elevator.lower().strip():
                item_loader.add_value("elevator", False)
            elif "fa-check" in elevator.lower().strip():
                item_loader.add_value("elevator", True)
    
        furnished =response.xpath("//tr[td[contains(.,'Opleveringsniveau')]]/td[2]/text()").extract_first()    
        if furnished:
            if "gemeubileerd" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
      
        desc = " ".join(response.xpath("//div[contains(.,'Omschrijving')]/following-sibling::*/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Altijd Wonen BV")
        item_loader.add_value("landlord_phone", "030-243 98 17")
        item_loader.add_value("landlord_email", "info@altijdwonen.nl")

        yield Request(
            url=response.xpath("//iframe[@id='gmap_canvas']/@src").get(),
            callback=self.maps_page,
            headers={
                "upgrade-insecure-requests": "1",
                "referer": "https://www.altijdwonen.nl/",
            },
            meta={
                "item":item_loader,
            }
        )
    

    def maps_page(self, response):
        item_loader = response.meta["item"]

        script_data = response.xpath("//script[contains(.,'onEmbedLoad')]//text()").get()
        if script_data:
            latlng = script_data.split('",[')[1].split("]]")[0].strip()
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None