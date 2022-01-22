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
    name = 'trouverie_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Trouverie_Immobilier_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.pozzo-immobilier.fr/louer/nos-offres/appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.pozzo-immobilier.fr/louer/nos-offres/maison",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response): 
        page=response.meta.get("page",2)
        seen=False
        stop=response.xpath("//div[@class='pagination__items pagination__items--pages']//span[last()]/text()").get()


        for item in response.xpath("//div[@class='d-flex']/following-sibling::a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen=True
        if seen or page==2:
            if stop and page<=int(stop)+1:
                url=f"https://www.pozzo-immobilier.fr/louer/nos-offres/appartement?page={page}"
                yield Request(url, callback=self.parse, meta={"property_type":response.meta["property_type"],"page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_source",self.external_source)
        external_id=item_loader.get_output_value("title")
        if external_id:
            item_loader.add_value("external_id",external_id.split("réf.")[-1].strip())
        city=response.xpath("//div[.='Ville']/following-sibling::div/text()").get()
        if city:
            item_loader.add_value("address", city.split("(")[0].strip())
            item_loader.add_value("city", city.split("(")[0].strip())
      
        zipcode=response.xpath("//div[.='Ville']/following-sibling::div/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].strip().split(")")[0])

        square_meters=response.xpath("//strong[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        description=response.xpath("//h2[.='Présentation du bien']/following-sibling::div/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//strong[contains(.,'pièces')]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0].split("p")[0].replace("\xa0",""))
        bathroom_count=response.xpath("//strong[contains(.,'sdb / sde')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0].split("\xa0")[0])
        rent=response.xpath("//div[@class='h3 m-0 text-primary']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ","").strip())
        item_loader.add_value("currency","EUR")
        deposit=response.xpath("//div[.='Dépôt de garantie']/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","").split(",")[0])
        images=[response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper']//div/a/@href").getall()]
        if images:
            for i in images:
                if not "node" in i and "jpg" in i:
                    item_loader.add_value("images",i)
        floor=response.xpath("//div[.='Etage']/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        utilities=response.xpath("//div[.='Dont honoraires état des lieux']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(",")[0])
        
        item_loader.add_xpath("address", "//meta[@property='og:locality']/@content")
       
        landlord_name= "".join(response.xpath("//div[@class='d-flex align-items-center font-weight-bold']//text()[normalize-space()]").getall())
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.strip())
        item_loader.add_xpath("landlord_email", "//meta[@property='og:phone_number']/@content")
        item_loader.add_xpath("landlord_phone", "//meta[@property='og:email']/@content")
        item_loader.add_xpath("latitude", "//meta[@property='place:location:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='place:location:longitude']/@content")
        
        # ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//h2[contains(.,'Dépenses énergétiques')]//following-sibling::img//@src[not(contains(.,'dpe-ni') or contains(.,'ges-'))]", input_type="F_XPATH",split_list={"dpe-":1,".":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Stationnement')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Meublé')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrasse')]//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'contact-block')]//strong//text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'contact-block')]//a[contains(@href,'tel')]//@href", input_type="F_XPATH", split_list={":":1})


        
        yield item_loader.load_item()