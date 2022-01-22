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

class MySpider(Spider):
    name = 'guisimmobilier_com'    
    execution_type='testing'
    country='france'
    locale='fr' 
    def start_requests(self):
        formdata = {
            "data[Search][offredem]": "2",
            "data[Search][idtype][]": "2",
            "data[Search][prixmax]": "",
            "data[Search][piecesmin]": "",
            "data[Search][NO_DOSSIER]": "",
            "data[Search][distance_idvillecode]": "",
            "data[Search][prixmin]": "",
            "data[Search][surfmin]": "",
        }
        yield FormRequest("http://www.guisimmobilier.com/recherche/",
                        callback=self.parse,
                        formdata=formdata,
                        meta={'property_type': "apartment"})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//ul[@class='listingUL']/li/@onclick").getall():
            follow_url = response.urljoin(item.split("href=")[1].replace("'", ""))
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://www.guisimmobilier.com/recherche/{page}"
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            yield Request(
                p_url,
                callback=self.parse,
                headers=headers,
                dont_filter=True,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )
        else:
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": "1",
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest("http://www.guisimmobilier.com/recherche/",
                            callback=self.jump,
                            formdata=formdata,
                            meta={'property_type': "house"})
    def jump(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//ul[@class='listingUL']/li/@onclick").getall():
            follow_url = response.urljoin(item.split("href=")[1].replace("'", ""))
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://www.guisimmobilier.com/recherche/{page}"
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            yield Request(
                p_url,
                callback=self.jump,
                headers=headers,
                dont_filter=True,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )


    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Guisimmobilier_PySpider_france")
        item_loader.add_xpath("external_id", "substring-after(//span[@itemprop='productID']//text()[normalize-space()],'Ref')")
        title ="".join( response.xpath("//div[@class='bienTitle']/h2//text()[normalize-space()]").extract() )
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        rent =" ".join(response.xpath("//div[@class='prix-dt2']//text()[contains(.,'€')]").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
        city = response.xpath("//div/p[contains(.,'Ville')]/span[@class='valueInfos ']//text()").extract_first()       
        if city:
            item_loader.add_value("city",city.strip())
        address = response.xpath("//section[contains(@class,'map-infos-city')]//h1/text()").extract_first()       
        if address:
            item_loader.add_value("address",address.strip())

        zipcode = response.xpath("//div/p[contains(.,'Code postal')]/span[2]//text()").extract_first()       
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        room_count = response.xpath("//div/p[contains(.,'chambre')]/span[@class='valueInfos ']//text()").extract_first() 
        if not room_count:   
            room_count = response.xpath("//div/p[contains(.,'pièces')]/span[@class='valueInfos ']//text()").extract_first() 
        if room_count:
            item_loader.add_value("room_count", room_count) 
 
        bathroom_count = response.xpath("//div/p[contains(.,'salle de bain') or contains(.,'salle d')]/span[2]//text()").extract_first()       
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count) 
        floor = response.xpath("//div/p[contains(.,'Etage')]/span[2]//text()").extract_first()       
        if floor:
            item_loader.add_value("floor", floor.strip()) 
        balcony = response.xpath("//div/p[contains(.,'Balcon')]/span[2]//text()").extract_first()       
        if balcony:
            if "NON" in balcony.upper():
                item_loader.add_value("balcony", False) 
            else:
                item_loader.add_value("balcony", True) 

        terrace = response.xpath("//div/p[contains(.,'Terrasse')]/span[@class='valueInfos ']//text()").extract_first()       
        if terrace:
            if "NON" in terrace.upper():
                item_loader.add_value("terrace", False) 
            else:
                item_loader.add_value("terrace", True) 
        parking = response.xpath("//div/p[contains(.,'garage')]/span[@class='valueInfos ']//text()").extract_first()       
        if parking:
            if "NON" in parking.upper():
                item_loader.add_value("parking", False) 
            else:
                item_loader.add_value("parking", True) 

        furnished = response.xpath("//div/p[contains(.,'Meublé')]/span[@class='valueInfos ']//text()").extract_first()       
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False) 
            else:
                item_loader.add_value("furnished", True) 

        elevator = response.xpath("//div/p[contains(.,'Ascenseur')]/span[@class='valueInfos ']//text()").extract_first()       
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False) 
            else:
                item_loader.add_value("elevator", True)      
  
        square = response.xpath("substring-before(//div/p[contains(.,'Surface habitable')]/span[@class='valueInfos ']//text(),'m')").extract_first()       
        if square:
            item_loader.add_value("square_meters", int(float(square.strip().replace(",",'.')))) 
        utilities = response.xpath("//div/p[contains(.,'Charges locatives')]/span[2]//text()").extract_first()       
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 
        deposit = response.xpath("//div/p[contains(.,'Dépôt de garantie')]/span[2]//text()").extract_first()       
        if deposit: 
            deposit = deposit.replace(" ","").replace("€","").strip()
            if deposit.replace(",","").isdigit():
                item_loader.add_value("deposit",int(float(deposit.replace(",","."))))

        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        script_map = response.xpath("//script//text()[contains(.,' lat') and contains(.,'lng')]").get()
        if script_map:  
            item_loader.add_value("latitude", script_map.split(' lat')[1].split(":")[1].split(',')[0].strip())
            item_loader.add_value("longitude", script_map.split('lng')[1].split(":")[1].split('}')[0].strip())
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src[not(contains(.,'no_bien'))]").extract()]
        if images:
            item_loader.add_value("images", images)    

        item_loader.add_value("landlord_phone", "04 96 11 22 88")
        item_loader.add_value("landlord_email", "contact@guisimmobilier.fr")
        item_loader.add_value("landlord_name", "Guis immobilier") 
        
        yield item_loader.load_item()

