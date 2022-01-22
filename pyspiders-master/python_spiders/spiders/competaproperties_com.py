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
    name = 'competaproperties_com'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://competaproperties.com/fr/component/osproperty/recherche-intensive.html?category_ids%5B%5D=1&property_type=3&keyword=&min_price=0&max_price=2501&address=&state_id=&sortby=a.price&orderby=desc&nbath=&nbed=&nfloors=&nroom=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fcompetaproperties.com%2F&limitstart=0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=101&search_param=catid%3A1_type%3A3_type%3A3_country%3A_max_price%3A2501_sortby%3Aa.price_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=0",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://competaproperties.com/fr/component/osproperty/recherche-intensive.html?category_ids%5B%5D=6&property_type=3&keyword=&min_price=0&max_price=1800000&address=&state_id=&sortby=a.price&orderby=desc&nbath=&nbed=&nfloors=&nroom=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fcompetaproperties.com%2F&limitstart=0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=101&search_param=catid%3A6_type%3A3_type%3A3_country%3A_max_price%3A1800000_sortby%3Aa.price_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=0",
                    "https://competaproperties.com/fr/component/osproperty/recherche-intensive.html?category_ids%5B%5D=4&property_type=3&keyword=&min_price=0&max_price=1800000&address=&state_id=&sortby=a.price&orderby=desc&nbath=&nbed=&nfloors=&nroom=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Fcompetaproperties.com%2F&limitstart=0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=101&search_param=catid%3A4_type%3A3_type%3A3_country%3A_max_price%3A1800000_sortby%3Aa.price_orderby%3Adesc&list_id=0&adv_type=3&show_advancesearchform=0",

                ],
                "property_type" : "house"
            },
            

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 15)

        seen = False
        for item in response.xpath("//figure[@class='pimage']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.split("&limitstart")[0] + f"&limitstart={page}" + (response.url.split("&limitstart=")[1])[response.url.split("&limitstart=")[1].find('&'):]
            yield Request(
                url=url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+15}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Competaproperties_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title=response.xpath("//div[@class='detail-title']/h1/text()").get()
        item_loader.add_value("title", title.strip())
        

        external_id="".join(response.xpath("//div/span[contains(.,'ID')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        room_count="".join(response.xpath("//div[@class='span6']/div/div[contains(.,'Chambres:')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        
        square_meters="".join(response.xpath("//div[@class='span6']/div/div[contains(.,'Surface')]/text()").getall())
        sq_m="".join(response.xpath("//div[@class='span6']/div/div[contains(.,'M2 construits')]/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].strip())
        elif sq_m:
            item_loader.add_value("square_meters", sq_m.split(":")[1].strip())
            
        rent=response.xpath("//div[@id='currency_div_vac']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        address=response.xpath("//div[@class='detail-title']/h1/text()").get()
        if address:
            address=address.split(" en ")[1]
            item_loader.add_value("address", address.split(",")[0])
            item_loader.add_value("city", address.split(",")[0])
                  
        desc="".join(response.xpath("//div[@id='detailstab']/div[1]//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        furnished=response.xpath("//div[@class='row-fluid']/div/div[contains(.,' Meublé')]/text()[normalize-space()]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        terrace=response.xpath("//div[@class='row-fluid']/div[@class='span12'][contains(.,'Terrasse')]/text()[normalize-space()] | //div[@class='row-fluid']/div/div[contains(.,'Terrasse')]/text()[normalize-space()]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony=response.xpath("//div[@class='row-fluid']/div[@class='span12'][contains(.,'Balcon')]/text()[normalize-space()]").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        dishwasher=response.xpath("//div[@class='row-fluid']/div/div[contains(.,'Lave-vaisselle')]/text()[normalize-space()]").get()
        washing_machine=response.xpath("//div[@class='row-fluid']/div/div[contains(.,'Lave-linge')]/text()[normalize-space()]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        swimming_pool=response.xpath("//div[@class='row-fluid']/div/div[contains(.,' Piscine')]/text()[normalize-space()]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
            
        images=[x for x in response.xpath("//figcaption/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        bathroom="".join(response.xpath(
            "//div[@class='span6']//div[contains(.,'Salles de bain')]//text()").getall())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split(":")[1].strip())
        
        
        item_loader.add_value("landlord_name","Competa Properties")
        item_loader.add_value("landlord_phone","34 952 516 107")
        item_loader.add_value("landlord_email","info@competaproperties.com")
       
        yield item_loader.load_item()

        
       

        
        
          

        

      
     