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
    name = 'easymakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.easymakelaars.nl/aanbod/woningaanbod/huur/"]

    # 1. FOLLOWING
    def parse(self, response):

        script_data = response.xpath("//script[contains(.,'aObjects =')]/text()").get()
        data = json.loads(script_data.split("aObjects =")[1].split(";")[0].strip())

        for item in data:
            follow_url = response.urljoin(item["link"])
            lat, lng = item["lat"], item["lng"]
            yield Request(follow_url, callback=self.populate_item, meta={"lat":lat, "lng":lng})
        
        next_page = response.xpath("//span[contains(@class,'next-page')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("latitude", str(response.meta.get('lat')))
        item_loader.add_value("longitude", str(response.meta.get('lng')))

        prop_type = response.xpath("//span[.='Soort object']/following-sibling::*/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            desc = "".join(response.xpath("//div[@class='ogDescription']/div/text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return
        item_loader.add_value("external_source", "Easymakelaars_PySpider_netherlands")
     
        title =response.xpath("//div[contains(@class,'addressInfo ')]/h1/text()").extract_first()
        if title:
            item_loader.add_value("title",title.strip() )       
        address =" ".join(response.xpath("//div[contains(@class,'addressInfo ')]//text()").extract())
        if address:
            item_loader.add_value("address",address.replace("\r\n","").replace("\t","").strip() )    
                
        item_loader.add_xpath("city","//div[contains(@class,'addressInfo ')]//span[@class='locality']/text()")                
        item_loader.add_xpath("zipcode","//div[contains(@class,'addressInfo ')]//span[@class='postal-code']/text()")                
        rent =response.xpath("//div/span[span[.='Huurprijs']]/span[@class='kenmerkValue']//text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   
         
        available_date = response.xpath("//div/span[span[.='Aanvaarding']]/span[@class='kenmerkValue']//text()").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Per",""), languages=['nl'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        floor = response.xpath("//div/span[span[contains(.,'woonlagen')]]/span[@class='kenmerkValue']//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.replace("woonlaag","").strip())      
        room_count = response.xpath("//div/span[span[contains(.,'slaapkamer')]]/span[@class='kenmerkValue']//text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        else:
            room_count = response.xpath("substring-after(//div[@id='Omschrijving']/text()[contains(.,'Slaapkamers')],': ')").get()
            if room_count:
                item_loader.add_value("room_count",room_count)
            else:
                room_count = response.xpath("//div/span[span[contains(.,'kamers')]]/span[@class='kenmerkValue']//text()").get()
                if room_count:
                    item_loader.add_value("room_count",room_count)
                
        bathroom_count = response.xpath("substring-after(//div[@id='Omschrijving']/text()[contains(.,'Badkamers:')],'Badkamers:')").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        square =response.xpath("//div/span[span[.='Woonoppervlakte']]/span[@class='kenmerkValue']//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        parking = response.xpath("//div/span[span[contains(.,'Parkeerfaciliteiten')]]/span[@class='kenmerkValue']//text()").extract_first()       
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//div/span[span]/span[@class='kenmerkValue']//text()[contains(.,'terras')]").extract_first()       
        if terrace:
            item_loader.add_value("terrace", True) 
        furnished = response.xpath("//div/span[span]/span[@class='kenmerkValue']//text()[contains(.,'Gemeubileerd')]").extract_first()       
        if furnished:
            item_loader.add_value("furnished", True)
        energy = response.xpath("//div/span[span[contains(.,'Energieklasse')]]/span[@class='kenmerkValue']//text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy)
        
        utilities =response.xpath("substring-before(//div[@id='Omschrijving']/text()[contains(.,'Service kosten') and contains(.,'€') and not(contains(.,'Kale huur'))],',')").extract_first()    
        if utilities:
            item_loader.add_value("utilities",utilities) 
        pets =response.xpath("//div[@id='Omschrijving']/text()[contains(.,'Pets:')]").extract_first()    
        if pets:
            if "no" in pets.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)

        deposit =response.xpath("substring-before(//div/span[span[.='Waarborgsom']]/span[@class='kenmerkValue']//text(),',')").extract_first()    
        if deposit:
            item_loader.add_value("deposit",deposit) 
        desc = " ".join(response.xpath("//div[@id='Omschrijving']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
   
        images = [response.urljoin(x)for x in response.xpath("//div[@class='detailFotos']/span[@class='fotolist ']/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "EasyMakelaars B.V.")
        item_loader.add_value("landlord_phone", "071-5690569")
        item_loader.add_value("landlord_email", "welkom@easymakelaars.nl")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "Woonhuis" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None