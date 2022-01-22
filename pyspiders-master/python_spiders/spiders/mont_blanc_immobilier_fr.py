# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_comments, remove_tags
from python_spiders.loaders import ListingLoader
import json 
import re

class MySpider(Spider):
    name = 'mont_blanc_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mont-blanc-immobilier.fr/location/annee/",
                ],
                "property_type" : "apartment",
            },
            # {
            #     "url" : [
            #         "https://www.mont-blanc-immobilier.fr/louer/maison/t-12/1",
            #     ],
            #     "property_type" : "house"
            # },
        ]

        headers = {
            "Accept":"text/plain, */*; q=0.01",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Host":"www.mont-blanc-immobilier.fr",
            "Referer":"https://www.mont-blanc-immobilier.fr/",
            "sec-ch-ua-mobile":"?0",
            "Sec-Fetch-Dest":"empty",
            "Sec-Fetch-Mode":"cors",
            "Sec-Fetch-Site":"same-origin",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "X-Requested-With":"XMLHttpRequest",
        }
        
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            headers=headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        # yield{
        #     "data":response.body
        # }
        headers = {
            "Accept":"text/plain, */*; q=0.01",
            "Accept-Encoding":"gzip, deflate, br",
            "Accept-Language":"tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection":"keep-alive",
            "Host":"www.mont-blanc-immobilier.fr",
            "Referer":"https://www.mont-blanc-immobilier.fr/",
            "sec-ch-ua-mobile":"?0",
            "Sec-Fetch-Dest":"empty",
            "Sec-Fetch-Mode":"cors",
            "Sec-Fetch-Site":"same-origin",
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "X-Requested-With":"XMLHttpRequest",
        }

        page = response.meta.get("page", 2)
        seen = False 

        for item in response.xpath("//div[@class='Card_Medias']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item,headers=headers, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            url = f"https://www.mont-blanc-immobilier.fr/location/annee/p{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1,"property_type":"apartment"})

    # # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mont_Blanc_Immobilier_PySpider_france")
        title = "".join(response.xpath("//h2/text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
        dontlet= response.url
        if dontlet and "garage" in dontlet:
            return  
        externalid=response.xpath("//h3/text()").get()
        if externalid:
            externalid=externalid.split("/")[-1]
            externalid=re.findall("\d+",externalid)
            item_loader.add_value("external_id", externalid)

        rent=response.xpath("//h2/text()").get()
        if rent:
            rent=rent.split("/")[-1].replace(" ","")
            rent=re.findall("\d+",rent) 
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency","EUR")
        adres=item_loader.get_output_value("title")
        if adres:
            item_loader.add_value("address",title.split("/")[1])
            item_loader.add_value("city",title.split("/")[1])



        desc=response.xpath("//div[@class='topSpacer']/text()").getall()
        if desc:
            item_loader.add_value("description", desc)
        utilities=item_loader.get_output_value("description")
        if utilities:
            utilities=utilities.split("Loyer")[-1].split("+")[-1].split("c")[0].replace("\u20ac","").strip()
            utilities=re.findall("\d+",utilities)
            item_loader.add_value("utilities",utilities)
        images=[x for x in response.xpath("//div[@class='Slide']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        square_meters=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Surface habitable')]/following-sibling::td/text()").get() 
        if square_meters:
            squ=re.findall("\d+",square_meters)
            item_loader.add_value("square_meters", squ)
        parking=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Parking')]/following-sibling::td/text()").get() 
        if parking:
            item_loader.add_value("parking", True)

        room_count=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Chambres')]/following-sibling::td/text()").get() 
        if room_count:
            item_loader.add_value("room_count", room_count)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Pièces')]/following-sibling::td/text()").get()
            if room1:
                item_loader.add_value("room_count",room1)
        bathroom_count=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Salle d')]/following-sibling::td/text()").get() 
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        balcony=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Balcon')]/following-sibling::td/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        floor=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'Etage')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        elevator=response.xpath("//table[@class='Tr_Caract_Tbl']//td[contains(.,'ascenseurs')]/following-sibling::td/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        energy_label=response.xpath("substring-before(substring-after(//h5/following-sibling::div/img/@src,'NRJ'),'.')").get()
        if energy_label  :
            item_loader.add_value("energy_label",energy_label)
        
        
        item_loader.add_value("landlord_name", "MONT-BLANC IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 50 93 51 77")


        
        yield item_loader.load_item()