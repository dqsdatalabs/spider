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
    name = 'bsi_ogipa_com'    
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
 
    }
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"2", "studio":"4"}

        for key, value in kwargs.items():
            formdata = {
                "data[Search][offredem]": "2",
                "data[Search][idtype][]": value,
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest("https://www.bsi-ogipa.com/recherche/",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': key})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='block-link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.bsi-ogipa.com/recherche/{page}"
            yield Request(
                p_url,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+2}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bsi_Ogipa_PySpider_france")
        item_loader.add_xpath("external_id", "substring-after(//span[@itemprop='productID']//text()[normalize-space()],':')")
        title ="".join( response.xpath("//div[@class='infosDt4Haut row']/h2//text()[normalize-space()]").extract() )
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        rent =" ".join(response.xpath("//div[@class='infosDt4Haut row']//span[contains(.,'€')]//text()").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
        city = response.xpath("//div/p[contains(.,'Ville')]/span//text()").extract_first()       
        if city:
            item_loader.add_value("city",city.strip())
        address = response.xpath("//section[contains(@class,'map-infos-city')]//h1/text()").extract_first()       
        if address:
            item_loader.add_value("address",address.strip())

        zipcode = response.xpath("//div/p[contains(.,'Code postal')]/span//text()").extract_first()       
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())

        room_count = response.xpath("//div/p[contains(.,'chambre')]/span//text()").extract_first() 
        if not room_count:   
            room_count = response.xpath("//div/p[contains(.,'pièce')]/span//text()").extract_first()       
        if room_count:
            item_loader.add_value("room_count", room_count) 
 
        bathroom_count = response.xpath("//div/p[contains(.,'salle de bain') or contains(.,'salle d')]/span//text()").extract_first()       
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count) 
        floor = response.xpath("//div/p[contains(.,'Etage')]/span//text()").extract_first()       
        if floor:
            item_loader.add_value("floor", floor.strip()) 
        balcony = response.xpath("//div/p[contains(.,'Balcon')]/span//text()").extract_first()       
        if balcony:
            if "NON" in balcony.upper():
                item_loader.add_value("balcony", False) 
            else:
                item_loader.add_value("balcony", True) 

        terrace = response.xpath("//div/p[contains(.,'Terrasse')]/span//text()").extract_first()       
        if terrace:
            if "NON" in terrace.upper():
                item_loader.add_value("terrace", False) 
            else:
                item_loader.add_value("terrace", True) 

        furnished = response.xpath("//div/p[contains(.,'Meublé')]/span//text()").extract_first()       
        if furnished:
            if "NON" in furnished.upper():
                item_loader.add_value("furnished", False) 
            else:
                item_loader.add_value("furnished", True) 

        elevator = response.xpath("//div/p[contains(.,'Ascenseur')]/span//text()").extract_first()       
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False) 
            else:
                item_loader.add_value("elevator", True)      
  
        square = response.xpath("substring-before(//div/p[contains(.,'Surface habitable')]/span//text(),'m')").extract_first()       
        if square:
            item_loader.add_value("square_meters", square) 
        utilities = response.xpath("//div/p[contains(.,'Charges locatives')]/span//text()").extract_first()       
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 
        deposit = response.xpath("//div/p[contains(.,'Dépôt de garantie')]/span//text()").extract_first()       
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","")) 
      
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

        item_loader.add_value("landlord_phone", "01 42 27 92 92")
        item_loader.add_value("landlord_email", "info@bsi-ogipa.fr")
        item_loader.add_value("landlord_name", "BSI OGIPA")    
        yield item_loader.load_item()

