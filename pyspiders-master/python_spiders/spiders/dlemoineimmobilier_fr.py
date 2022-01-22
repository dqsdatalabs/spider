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
    name = 'dlemoineimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
             {
                 "url" : "http://www.dlemoineimmobilier.fr/recherche?a=2&b%5B%5D=appt&b%5B%5D=build&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                 "property_type" : "apartment"
             },
             {
                 "url" : "http://www.dlemoineimmobilier.fr/recherche?a=2&b%5B%5D=house&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                 "property_type" : "house"
             },
            #{
            #   "url" : "http://www.dlemoineimmobilier.fr/search.php?a=1&b%5B%5D=appt&b%5B%5D=house&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&view=&ajax=1&facebook=1&start=0&&_=1603194266980",
            #    "property_type" : "property_for_sale"
            #    #SALE 
            #},

        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 12)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'res_tbl')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True

        #pagination sale
        if page == 12 or seen:
            if response.meta.get("property_type") == "property_for_sale":
                url = f"http://www.dlemoineimmobilier.fr/search.php?a=1&b%5B%5D=appt&b%5B%5D=house&b%5B%5D=build&b%5B%5D=pro&b%5B%5D=comm&b%5B%5D=bail&b%5B%5D=park&b%5B%5D=land&c=&radius=0&d=1&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&view=&ajax=1&facebook=1&start={page}&&_=1603266516312"
                yield Request(
                    url=url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page": page+12}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Dlemoineimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title","//div[@class='pageTitle']/h1/text()")

        address="".join(response.xpath("//div[@class='tech_detail']/table/tr[contains(.,'Ville')]//following-sibling::td//text()").getall())
        if address:
            item_loader.add_value("address", address)
                
        square_meters=response.xpath("//div[@class='tech_detail']/table/tr[contains(.,'Surface')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
                        
        room_count=response.xpath("//div[@class='tech_detail']/table/tr[contains(.,'Pièces')]//following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
                
        rent=response.xpath("//div[@id='value_prod']/div/table/tr/td/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
                
        zipcode=response.xpath("//div[@class='tech_detail']/table/tr[contains(.,'Ville')]//following-sibling::td/span[2]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        furnished=response.xpath("//div[@class='tech_detail']/table/tr[contains(.,'Ameublement')]//following-sibling::td/text()").get()
        if furnished and furnished!='Non meublé':
            item_loader.add_value("furnished", True)
        
        energy_label=response.xpath("//div[@class='dpe-letter']/b/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[0].strip())
        
        desc="".join(response.xpath("//div[@id='details']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images=[x for x in response.xpath("//div[@id='layerslider']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
          
        yield item_loader.load_item()

        
       

        
        
          

        

      
     