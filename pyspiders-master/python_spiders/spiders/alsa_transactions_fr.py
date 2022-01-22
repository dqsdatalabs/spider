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
import math
class MySpider(Spider):
    name = 'alsa_transactions_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Alsatransactions_PySpider_france_fr"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.alsa-transactions.fr/location/maison?prod.prod_type=house",
                "property_type" : "house"
            },
            {
                "url" : "https://www.alsa-transactions.fr/location/appartement?prod.prod_type=appt",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("///a[@class='_gozzbg']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        if page == 2 or seen:            
            p_url = f"https://www.alsa-transactions.fr/location/appartement?prod.prod_type=appt&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title","//title//text()")
        price =response.xpath("//text()[contains(.,'Loyer')]").get()
        if price:
            price = price.split("Loyer")[-1].split("€")[0].split(":")[-1].strip()
            item_loader.add_value("rent", price)
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent=response.xpath("//text()[contains(.,'provision pour charges')]").get()
            if rent:
                item_loader.add_value("rent",rent.split("€")[0].replace("\xa0","").strip())
        item_loader.add_value("currency", "EUR")
        external_id=response.xpath("//span[.='Référence']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        adres=response.xpath("//span[.='Localisation']/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("city",adres.split(" ")[0])
            item_loader.add_value("zipcode",adres.split(" ")[-1])
        square_meters=response.xpath("//span[.='Surface : ']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        room_count=response.xpath("//span[.='Chambres']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Salle d')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        description="".join(response.xpath("//span[@class='_1ta8edj _5k1wy textblock ']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        elevator=response.xpath("//span[.='Chambres']/following-sibling::span/text()").get()
        if elevator and "Oui" in elevator:
            item_loader.add_value("elevator",True)
        deposit=response.xpath("//span[@class='_1ta8edj _5k1wy textblock ']//text()[contains(.,'Dépôt de garantie')] | //u[.='Dépôt de garantie :']/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].split("€")[0].strip())
        utilities=response.xpath("//span[@class='_1ta8edj _5k1wy textblock ']//text()[contains(.,'Charges')]").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("€")[0].strip())
        images=[x for x in response.xpath("//div[@class='_h6joaj image _1yfus1e']//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//span[@class='_1ta8edj _5k1wy textblock ']//text()[contains(.,'DPE')]").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[1])
        
 

        item_loader.add_value("landlord_phone", "06 26 50 38 20")
        item_loader.add_value("landlord_email", "linda@alsa-transactions.fr")
        item_loader.add_value("landlord_name", "Linda HAKKAR")

        yield item_loader.load_item()
