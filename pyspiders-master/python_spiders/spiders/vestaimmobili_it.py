# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import TEMPLATE
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'vestaimmobili_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Vestaimmobili_PySpider_italy"   
    formdata = {
        "__RequestVerificationToken": "BPDGz7IPi_P6gdxpfiWyMhyWmPRWwPHjfMRs_QM54wdQ0dO3gOdQdMJxRtyeorOuxPzub-8oVVHFrOpDR1M_uLTcruQvca66LWoEK6O9hlM1",
        "comuneSearch":"0",
        "zonaSearch": "0",
        "numeroLocaliSearch": "0",
        "inAffittoSearch": "True",
        "mqMinSearch":"",
        "mqMaxSearch": "",
        "prezzoMinSearch": "",
        "prezzoMaxSearch": "",
    }   
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
  
    def start_requests(self):
        start_urls = 'https://www.vestaimmobili.it/Common/SearchHandler.cshtml'
        yield FormRequest(url=start_urls,
                dont_filter=True,
                formdata=self.formdata,
                headers=self.headers,
                callback=self.parse
            )

    # 1. FOLLOWING
    def parse(self, response):
        border=response.xpath("//a[contains(text(),'»')]/@href").get() 
        border=border.split("Immobili/")[-1].split("/a")[0]
        page = response.meta.get('page', 2)       
        seen = False
        for item in response.xpath("//div[@class='property-row-picture-inner']/a/@href").getall():
            
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True      
        
        if page == 2 or page<=int(border):
            follow_url=f"https://www.vestaimmobili.it/immobili/{page}/a"
            yield Request(url=follow_url, dont_filter=True,callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h3[@class='property-detail-subtitle']//span[@id='lblPrezzo']/text()").get()
        if "commerciale" in response.url:
            return
        if rent:
            rent=rent.replace(",","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//h3[@class='property-detail-subtitle']//span[@id='lblMq']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        desc=" ".join(response.xpath("//div[@id='lblDescrizione']//p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        else:
            desc = response.xpath("//div[@id='lblDescrizione']/text()").get()
            if desc:
                item_loader.add_value("description",desc)
        images=[response.urljoin(x) for x in response.xpath("//form[@id='form-immobile']//div[@id='div_slide']//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        features=response.xpath("//h2[.='Dettagli']/following-sibling::div/ul//li//text()").getall()
        if features:
            for i in features:
                if "classe" in i.lower():
                    item_loader.add_value("energy_label",i.strip().split(" ")[-1])
                if "ascensore" in i.lower():
                    item_loader.add_value("elevator",True)
                if "arredato" in i.lower():
                    item_loader.add_value("furnished",True)
            
        latitude=response.xpath("//script[contains(.,'maps.LatLng')]").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("zoom")[0].split("maps.LatLng")[-1].split(",")[0].replace("(","").strip())
        longitude=response.xpath("//script[contains(.,'maps.LatLng')]").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("zoom")[0].split("maps.LatLng")[-1].split(")")[0].split(",")[-1].strip())
        item_loader.add_value("landlord_name","Vesta Real Estate")
        item_loader.add_value("landlord_phone","02 58118683")
        item_loader.add_value("landlord_email","info@vestaimmobili.it")


        room_count = find_room_count(response.url)
        item_loader.add_value("room_count",room_count)
        
        address1 = response.xpath("//span[@id='lblIndirizzo']/text()").get()
        if address1:
            address2 = response.xpath("//span[@id='lblComune']/text()").get()
            address = address1 + "-" + address2
            item_loader.add_value("address",address)

        city = response.xpath("//span[@id='lblComune']/text()").get()
        if city:
            item_loader.add_value("city",city)

        item_loader.add_value("property_type","apartment")

        bathroom_count = response.xpath("//span[@id='lblNumServizi']/text()").get()
        if bathroom_count:
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count",bathroom_count)
            else:
                bathroom_count = bathroom_count.split()[1]
                item_loader.add_value("bathroom_count",bathroom_count)

        properties = response.xpath("//ul[@class='property-detail-amenities']/li/text()").getall()
        if "Balcone" in str(properties):
            item_loader.add_value("balcony",True)


        if "auto" in str(properties):
            item_loader.add_value("parking",True)


        

        yield item_loader.load_item()



def find_room_count(url):
    if "bilocale" in url:
        return 2

    elif "trilocale" in url:
        return 3

    elif "quadrilocale" in url:
        return 4

    elif "cinque" in url:
        return 5

    elif "plurilocale" in url:
        return 8

    else:
        return 1