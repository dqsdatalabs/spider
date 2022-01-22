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
from datetime import datetime
class MySpider(Spider):
    name = 'argayon_immo_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    start_urls = ["https://www.argayon-immo.be/fr-BE/List/7?rls=1"]


    def parse(self, response):
        token = response.xpath("//input[@name='__RequestVerificationToken']/@value").get()
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "2"
            },
            {
                "property_type" : "house",
                "type" : "1"
            },
        ]
        for item in start_urls:
            formdata = {
                "__RequestVerificationToken": token,
                "SearchTypeRent": "true",
                "SearchTypeSell": "false",
                "sale-rent": "on",
                "EstateRef": "",
                "SelectedType": item["type"],
                "MinPrice": "",
                "MaxPrice": "",
                "Rooms": "0",
                "SortParameter": "0",
                "Furnished": "false",
                "InvestmentEstate": "false",
                "GroundMinArea": "",
            }
            yield FormRequest(
                "https://www.argayon-immo.be/fr-BE",
                callback=self.jump,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                })

    # 1. FOLLOWING
    def jump(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//a[@class='estate-thumb']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 1 or seen:
            p_url = f"https://www.argayon-immo.be/fr-BE/List/PartialListEstate/7?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&Rooms=0&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&CurrentPage={page}&MaxPage=18&EstateCount=219&SoldEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapMarker%5D&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined"
            yield Request(
                p_url,
                callback=self.jump,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        status = response.xpath("//div/h2[contains(.,'Biens à louer')]//text()").extract_first() 
        if status:
            return
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Argayon_Immo_PySpider_belgium")
        item_loader.add_xpath("external_id", "//tr[th[contains(.,'Référence')]]/td/text()")
        item_loader.add_xpath("floor", "//tr[th[contains(.,'Étages')]]/td/text()")
       
        title = response.xpath("//title/text()").extract_first() 
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
        
        address = ", ".join(response.xpath("//div[@class='estate-feature']/p/text()[1]/following-sibling::text()[normalize-space()]").extract() )
        if address:
            address = re.sub("\s{2,}", " ", address)    
            item_loader.add_value("address",address.strip() ) 
            address =address.split(", ")[-1].strip()
            zipcode = address.split(" ")[0]
            city = " ".join(address.split(" ")[1:])
            item_loader.add_value("city",city.strip() ) 
            item_loader.add_value("zipcode",zipcode.strip() ) 

        room_count = response.xpath("//span[i[contains(@class,'fw-bedroom')]]/text()").extract_first() 
        if room_count: 
            item_loader.add_value("room_count",room_count) 
 
        energy_label = response.xpath("substring-after(//div[@class='estate-feature']//img/@src,'9_')").extract_first() 
        if energy_label: 
            item_loader.add_value("energy_label",energy_label.split(".")[0].upper()) 
        bathroom_count = response.xpath("//span[i[contains(@class,'fw-bathroom')]]/text()").extract_first() 
        if bathroom_count: 
            item_loader.add_value("bathroom_count",bathroom_count) 

        rent = response.xpath("//span[@class='estate-text-emphasis']/text()").extract_first() 
        if rent: 
            item_loader.add_value("rent_string",rent)      
        utilities = response.xpath("//tr[th[contains(.,'Charges (€) (montant)')]]/td/text()").extract_first() 
        if utilities: 
            item_loader.add_value("utilities",utilities)  

        available_date = response.xpath("//div[h2[contains(.,'Description')]]/p//text()[contains(.,'Disponible')]").extract_first() 
        if available_date:
            available_date = available_date.split("Disponible")[1].split(".")[0]
            if "mmédiatemen" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace("mmédiatemen","now").strip(),date_formats=["%d-%m-%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
      
        square =response.xpath("//tr[th[contains(.,'Surface habitable')]]/td/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        furnished =response.xpath("//tr[th[contains(.,'Meublé')]]/td/text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)    
        terrace =response.xpath("//tr[th[contains(.,'Terrasse')]]/td/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)   
        elevator =response.xpath("//tr[th[.='Ascenseur']]/td/text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)  
        swimming_pool =response.xpath("//tr[th[contains(.,'Piscine')]]/td/text()").extract_first()    
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)  
        parking = " ".join(response.xpath("//tr[th[contains(.,'Parking') or contains(.,'Garage')]]/td/text()").extract())    
        if parking:
            if "oui" in parking.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False) 
        desc = " ".join(response.xpath("//div[h2[contains(.,'Description')]]/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [x for x in response.xpath("//ul[contains(@class,'slider-main-estate')]/li/a//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Argayon")
        item_loader.add_value("landlord_phone", "027930383")
        item_loader.add_value("landlord_email", "visite@argayon-immo.be")   
        
        yield item_loader.load_item()

