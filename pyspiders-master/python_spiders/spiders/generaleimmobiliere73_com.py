# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from datetime import datetime


class MySpider(Spider):
    name = 'generaleimmobiliere73_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.generaleimmobiliere73.com/location/maison",
                "property_type" : "house"
            },
            {
                "url" : "https://www.generaleimmobiliere73.com/location/appartement",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='immobilier--teaser__photo']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        next_page = response.xpath("//span[@class='pager-next__label']/../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Generaleimmobiliere73_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div//h1/text()")
   

        address_detail = response.xpath("//div[contains(@class,'grid__address')]/p//text()").extract_first()
        if address_detail:
            address=address_detail.split("- ")[1].strip()
            zipcode=address_detail.split("- ")[0].strip()
            if address:
                item_loader.add_value("address",address)
                item_loader.add_value("city",address)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
        
        price = response.xpath("//div/p[@class='h1 my-0']//text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ","."))

        external_id = response.xpath("//div/span[contains(.,'Référence')]/following-sibling::span//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())       
  
        room_count = response.xpath("//div/span[contains(.,'Chambre')]/following-sibling::span//text()[.!='N.C.']").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count )
        else:
            room_count = response.xpath("//div/span[contains(.,'pièce')]/following-sibling::span//text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count )
     
        square = response.xpath("//div/span[contains(.,'Surface')]/following-sibling::span//text()").extract_first()
        if square:
            square_meters = square.split("m")[0]
            item_loader.add_value("square_meters",square_meters.strip() )

        bathroom_count = response.xpath("//div/span[contains(.,'bain')]/following-sibling::span//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//div/span[contains(.,'Etage')]/following-sibling::span//text()[.!='N.C.']").extract_first()
        if floor:
            item_loader.add_value("floor", floor)

        LatLng = "".join(response.xpath("substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'lat')],'lat'),',')").extract())
        if LatLng:
            lat = LatLng.replace('":"','').replace('"','')
            lng = "".join(response.xpath("substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'lng')],'lng'),'}')").extract())
            long_lng = lng.replace('":"','').replace('"','')
            item_loader.add_value("latitude", lat.strip())
            item_loader.add_value("longitude", long_lng)
        
        
        a_date = response.xpath("//div/span[contains(.,'Disponibilité')]/following-sibling::span//text()[.!='Disponible']").extract_first()
        if a_date:
            datetimeobject = datetime.strptime(a_date,'%d/%m/%Y')
            newformat = datetimeobject.strftime('%Y-%m-%d')
            item_loader.add_value("available_date", newformat)
        
        deposit = response.xpath("//div/span[contains(.,'Dépôt de garantie')]/following-sibling::span//text()").extract_first()
        if deposit:   
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ",""))
        
        utilities = response.xpath("//div/span[contains(.,'charges')]/following-sibling::span//text()").extract_first()
        if utilities:   
            item_loader.add_value("utilities", utilities.split(" ")[0].strip())
        
        desc = "".join(response.xpath("//div[contains(@class,'description')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "balcon" in desc:
                item_loader.add_value("balcony", True)
  
        elevator = response.xpath("//div/span[contains(.,'Ascenseur')]/following-sibling::span//text()").extract_first()
        if elevator:
            if elevator=="Non":
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
                 
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'immobilier--full__gallery__item')]/img/@src | //div[@class='immobilier--full__gallery__slider']/img/@src").extract()]
        if images :
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "04 79 96 06 66")
        item_loader.add_value("landlord_name", "Générale Immobilière")
        item_loader.add_value("landlord_email", "contact@generaleimmobiliere73.com")
        
        yield item_loader.load_item()

