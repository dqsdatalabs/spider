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
    name = 'barnes_london_com_disabled'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en" 

    def start_requests(self):
 
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "appartement",
            },
            {
                "property_type" : "house",
                "type" : "house",
            },
        ]
        for item in start_urls:
            formdata = {
                "FormSearchPays": "GB",
                "FormSearchTypeannonce": "location",
                "typebien[]": item['type'],
                "prix_min": "",
                "prix_max": "",
                "FormSearchPaysOld": "GB",
                "post_redirect_get": "y",
                "FormSearchLocalisation": "",
                "LocalisationchoisiIdBloc": "0",
                "req_tri": "DEFAULT",
                "req_typerecherche": "liste",
                "select_lang": "UK",
                "FormSearchLocalisation_intern": "",
            } 
            api_url = "https://www.barnes-london.com/en/for-rent/"
            yield FormRequest( 
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                })
     
    p_state = False

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 28)
        seen = False
        for item in response.xpath("//h2[@class='h4']/../@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 28 or seen:
            p_url = f"https://www.barnes-london.com/v5_php/moteur.php?ajx=ok&start={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+28,
                })

        elif not self.p_state:
            self.p_state = True
            formdata = {
                "FormSearchPays": "GB",
                "FormSearchTypeannonce": "location",
                "typebien[]": "maison/villa",
                "prix_min": "",
                "prix_max": "",
                "FormSearchPaysOld": "GB",
                "post_redirect_get": "y",
                "FormSearchLocalisation": "",
                "LocalisationchoisiIdBloc": "0",
                "req_tri": "DEFAULT",
                "req_typerecherche": "liste",
                "select_lang": "UK",
                "FormSearchLocalisation_intern": "",
            }
            api_url = "https://www.barnes-london.com/en/for-rent/"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":"house",
                })

        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_source", "Barnes_London_PySpider_united_kingdom")

        external_id = "".join(response.xpath("//div[@class='text-muted my-4']/text()").getall())
        if external_id:
            item_loader.add_value("external_id",external_id.split(" ")[1].strip()) 
  
          
        # rent=response.xpath("//ul[@class='list-grid']/li/div[@class='text-xl']/text()").get()
        rent=response.xpath("//div[@class='anim-fade-up delay-md']/h1/text()").get()
        
        if rent:
            rent=rent.split("€")[1].split("/")[0].replace(",","").strip()
            item_loader.add_value("rent", int(rent)*4)
             
  
        # rent = response.xpath("//div[@class='btn-wrapper btn-wrapper--twin mt-5']/div/text()").get()

        # if rent:
        #     if "week" in rent.lower():
        #         price = rent.split("€")[1].split("/")[0].replace(",","")
        #         item_loader.add_value("rent", int(price)*4)
        #     elif "month" in rent.lower():
        #         item_loader.add_value("rent",rent ) 
            #if not 'BAI180162' in response.url: return
            #with open("d", "wb") as f: f.write(response.body)
            # if 'week' in rent.lower(): item_loader.add_value("rent", int("".join(filter(str.isnumeric, rent))) * 4)
            # elif 'month' in rent.lower(): item_loader.add_value("rent", int("".join(filter(str.isnumeric, rent))))    
        item_loader.add_value("currency","EUR")
        
        bathroom_count = response.xpath("//div[contains(text(),'Bathroom')]/following-sibling::div/text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip()) 

        room_count = "".join(response.xpath("//ul[@class='list-inline text-muted my-4']/li/i[contains(@class,'fa-bed')]/following-sibling::span/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0].strip()) 
        else:
             item_loader.add_xpath("room_count","//li[div[contains(.,'Room')]]/div[2]/text()")

        address = "".join(response.xpath("//li[div[.='City']]/div[2]/text()").getall())
        if address:
            item_loader.add_value("address",address.strip()) 
            item_loader.add_value("city",address.strip().split(" ")[0].strip()) 
            item_loader.add_value("zipcode",address.strip().split(" ")[1].strip()) 

        description = " ".join(response.xpath("//p[@class='h6 text-muted']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())


        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'LatLng')],'LatLng('),','),')')")

        meters = response.xpath("//li[div[.='Area']]/div[2]/text()").extract_first()
        if meters:
            sqm = str(int(float(meters) * 0.09290304))
            item_loader.add_value("square_meters", sqm)

        images = [x for x in response.xpath("//div[@class='item']/picture/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = "".join(response.xpath("//li[div[.='Furnished']]/div[2]/text()").getall())
        if furnished:
            if "yes" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "no" in furnished.lower():
                item_loader.add_value("furnished",False)

        name = response.xpath("//div[@class='anim-fade-up delay-sm']/p[@class='h3']/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
        else:
            item_loader.add_value("landlord_name", "Barnes London")
        item_loader.add_xpath("landlord_phone", "//div[@class='anim-fade-up delay-sm']/p[@class='h3']/span/text()")
    
        yield item_loader.load_item()
