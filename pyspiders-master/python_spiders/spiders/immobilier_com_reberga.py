# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'immobilier_com_reberga'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Rebergaimmobilier_PySpider_france_fr"
    post_url = "https://reberga-immobilier.com/fr/recherche"
    current_index = 0
    other_prop = ["Maison|2"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "search-form-86792[search][category]": "Location|2",
            "search-form-86792[search][type][]": "Appartement|1",
            "search-form-86792[search][price_min]": "",
            "search-form-86792[search][price_max]": "",
            "search-form-86792[submit]": "",
            "search-form-86792[search][order]": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//ul[@class='_list listing']//li//a[.='Voir le bien']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            follow_url = response.urljoin(next_page)
       
            yield Request(
                    url=follow_url,
                    callback=self.parse,
                    meta={"property_type":response.meta["property_type"], "page":page+1}
                )
        elif self.current_index < len(self.other_prop):
            formdata = {
               
                "search-form-86792[search][category]": "Location|2",
                "search-form-86792[search][type][]": self.other_prop[self.current_index],
                "search-form-86792[search][price_min]": "",
                "search-form-86792[search][price_max]": "",
                "search-form-86792[submit]": "",
                "search-form-86792[search][order]": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1



    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        external_id= response.xpath("//p[contains(., 'Réf.')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[-1].strip())

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace("\u00e9",""))
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
      
        address=response.xpath("//div[contains(@class,'info-template-2')]/h1/text()[last()]").get()
        if address:
            item_loader.add_value("address", address.strip().replace("\u00e9",""))   
            item_loader.add_value("city", address.strip().replace("\u00e9",""))   
        
        square_mt=response.xpath("//p[contains(., 'm²')]//text()").get()
        if square_mt:
            item_loader.add_value("square_meters", square_mt.split("m²")[0].strip())        
        room_count=response.xpath("//p//text()[contains(., 'chambre')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split("chambre")[0])  
        else:
            room_count=response.xpath("//p[contains(., 'pièce')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])        
        bathroom_count=response.xpath("//p//text()[contains(., 'salle d')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split("salle d")[0])  
        rent=response.xpath("//p[contains(., '€')][1]//text()").get()
        if rent:
            price = rent.split("€")[0]
            item_loader.add_value("rent",price)
            item_loader.add_value("currency", "EUR")       
     
            
        deposit=response.xpath("//li[contains(., 'Dépôt de garantie')]//following-sibling::span//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0])
        utilities =response.xpath("//li[contains(., 'Provision sur charge')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
             
        desc="".join(response.xpath("//p[@id='description']//text()").get())
        if desc:
            item_loader.add_value("description", desc.replace("\u00e9",""))

        images=[x for x in response.xpath("//div[@class='slider']//img/@src | //div[@class='slider']//img/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)
        landlord_name= response.xpath("//ul[@class='listing']//h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone= response.xpath("//ul[@class='listing']//a[contains(@href,'tel')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        landlord_email= response.xpath("//ul[@class='listing']//a[contains(@href,'mail')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        
        yield item_loader.load_item()

        
          

        

      
     