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
import re

class MySpider(Spider):
    name = 'labelimmo_be'
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.labelimmo.be/biens/a-louer?option=com_labelimmo&view=category&reference=&category=apartment&room=&price=&bath=&front=&orderby=new&purpose=2&pagination=18&page=1&Itemid=137&aa3ee24b4f101621eec3df884ad344aa=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.labelimmo.be/biens/a-louer?option=com_labelimmo&view=category&reference=&category=house&room=&price=&bath=&front=&orderby=new&purpose=2&pagination=18&page=1&Itemid=137&aa3ee24b4f101621eec3df884ad344aa=1",
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
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='picture']"):
            status = item.xpath("./div/img/@alt").get()
            if status and "loué" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//a[contains(@class,'uk-button-primary')]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.labelimmo.be/biens/a-louer?option=com_labelimmo&view=category&reference=&category={response.meta['property_type']}&room=&price=&bath=&front=&orderby=new&purpose=2&pagination=18&page={page}&Itemid=137&aa3ee24b4f101621eec3df884ad344aa=1"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )          
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Labelimmo_PySpider_belgium")
        main_block_xpath = ".//div[@class='w-detail']"

        detail_node_xpath = ".//div[@class='w-boxinfocenter']"
        item_loader.add_xpath("external_id", f"{main_block_xpath}//p[@class='w-ref']/span/text()")
        item_loader.add_xpath("title", f"{main_block_xpath}//h2[@class='title']//text()")

        item_loader.add_xpath("address", f"{main_block_xpath}//*[contains(@class,'w-adresse')]//text()")
        address = response.xpath(f"{main_block_xpath}//*[contains(@class,'w-adresse')]//text()").get()
        if address:
            address = address.split(",")
            if len(address) == 2:
                address = address[1]
                address = address.strip().split(" ")
                item_loader.add_value("zipcode", address[0])
                item_loader.add_value("city", " ".join(address[1:]))
        

        desc = "".join(response.xpath("//div[@class='uk-grid uk-grid-match uk-flex uk-flex-center']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            item_loader.add_xpath("description", f"{main_block_xpath}//div[h2[.='Présentation du bien']]/p[1]//text()")

        item_loader.add_xpath("images", ".//div[@id='ip-image-tab']/ul//a/@href")

        item_loader.add_xpath("rent_string", f"{main_block_xpath}//h3[@class='price']/text()")
        item_loader.add_xpath("room_count", f"{detail_node_xpath}//li[contains(.,'Chambre(s)')]/strong/text()")
        item_loader.add_xpath("bathroom_count", f"{detail_node_xpath}//li[contains(.,'Salle(s) de bain')]/strong/text()")
        item_loader.add_xpath("square_meters", f"{detail_node_xpath}//li[contains(.,'Surface hab')]/strong/text()")
        item_loader.add_xpath("floor", f"{detail_node_xpath}//li[contains(.,'Étage(s)')]/strong/text()")

        parking = response.xpath(f"{detail_node_xpath}//li[contains(.,'Parking') or contains(.,'Garage') ]/strong[.!='Non']/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
        terrace = response.xpath(f"{detail_node_xpath}//li[contains(.,'Terrasse')]/strong/text()").get()
        if terrace:
            item_loader.add_value("terrace", terrace != "Non")

        response.xpath(".//div[@class='w-boxinfocenter']")
        name = response.xpath(".//img[@class='maincarte']/@src").get()
        if name:
            name = name.split("/")[-1].replace(".png", "").replace("_", " ")
            name = name.split(" ")
            if len(name) == 2:
                name = " ".join([name[1].capitalize(), name[0].capitalize()])
            else:
                name = name[0].capitalize()
            item_loader.add_value("landlord_name", name)
        item_loader.add_xpath("landlord_email", ".//input[contains(@name,'contactform[officeMail]')]/@value")
        item_loader.add_xpath("landlord_phone", f"{main_block_xpath}//p[i[@class='uk-icon-phone']]/text()[1]")
        if response.xpath(".//iframe[@class='embedGoogleMap']/@src").get():
            yield Request(
                response.xpath(".//iframe[@class='embedGoogleMap']/@src").get(),
                self.parse_map,
                dont_filter=True,
                cb_kwargs=dict(item_loader=item_loader),
            )
        else:
            yield item_loader.load_item()
    
    def parse_map(self, response, item_loader):
        """read geo from text"""
        geo = re.search(r"\[\d+\.\d{5,},\d+\.\d{5,}\]", response.text)
        if geo:
            geo = geo.group()[1:-2].split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])
        yield item_loader.load_item()