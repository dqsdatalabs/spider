# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re


class MySpider(Spider): 
    name = 'atalaysaral_net'
    start_urls = ['http://www.atalaysaral.net/']  # LEVEL 1
    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source='Atalaysaral_PySpider_turkey_tr'
    custom_settings = {
        #"PROXY_ON": True,
        #"PROXY_PR_ON": True,
        #"PASSWORD": "wmkpu9fkfzyo",
        "HTTPCACHE_ENABLED": False,
    }


    def parse(self, response):
        view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
        view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
        event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()
        posx = response.xpath("//input[@name='__SCROLLPOSITIONX']/@value").get()
        posy = response.xpath("//input[@name='__SCROLLPOSITIONY']/@value").get()

        formdata = {
            "__EVENTTARGET": "ctl00$content$ctlSearchWidget1$btnSearch",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state,
            "__VIEWSTATEGENERATOR": view_state_gen,
            "__SCROLLPOSITIONX": posx,
            "__SCROLLPOSITIONY": posy,
            "__EVENTVALIDATION": event_val,
            "ctl00$content$ctlSearchWidget1$lstRealtyCategoryID": "1000010102",
            "ctl00$content$ctlSearchWidget1$hdnCityID": "",
            "ctl00$content$ctlSearchWidget1$hdnCityDescription": "",
            "ctl00$content$ctlSearchWidget1$hdnCountyIDs": "",
            "ctl00$content$ctlSearchWidget1$hdnDistrictIDs": "",
            "ctl00$content$ctlSearchWidget1$numStartPrice": "",
            "ctl00$content$ctlSearchWidget1$numEndPrice": "",
            "ctl00$content$ctlSearchWidget1$lstCurrency": "1",
            "ctl00$content$ctlSearchWidget1$numStartSqm": "",
            "ctl00$content$ctlSearchWidget1$numEndSqm": "",
            "ctl00$content$ctlSearchWidget1$txtRealtyKeyword": "",
            "ctl00$content$ctlSearchByRealtyNo1$numRealtyNo": "",
        }

        yield FormRequest(
            "http://www.atalaysaral.net/",
            callback=self.jump,
            formdata=formdata,
        )

    def jump(self, response):

        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//div[@id='content_uppList']/div[@class='item']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='title']/@href").get())
            property_type = item.xpath(".//span[contains(@id,'content_rptRealtyList_lblRealtyCategoryType')]/text()").get()
            if property_type and "Residence" in property_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"prop_type":property_type})
            elif property_type and "Daire" in property_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"prop_type":property_type})
            seen = True
        
        if page == 1 or seen:
            url = f"http://www.atalaysaral.net/kiralik-daireler/eRNA3xMId--o6NjOxkWPUd6fVSvxs6C2XWy7c2h7Tvl2e20e5bwFJA==&new=1&page={page}"
            yield Request(url, callback=self.jump, meta={"page": page+1})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Atalaysaral_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1//span//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
   
        external_id=response.xpath("//dl[@class='realty-details']/dd[1]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.replace("\n","").replace("\r","").strip())
        property_type = response.meta.get("prop_type")
        if property_type:
            item_loader.add_value("property_type", property_type)
           
        room=response.xpath("//dl/div/dt[span[.='Oda Sayısı']]//following-sibling::dd/text()").extract_first().split()
        item_loader.add_value("room_count", str(int(room[0])+ int(room[2])))
        bathroom=response.xpath("//dl/div/dt[span[.='Banyo Sayısı']]//following-sibling::dd/text()").extract_first()
        item_loader.add_value("bathroom_count",bathroom)
        city=response.xpath("//h2/span[3]/text()").extract_first()
        district=response.xpath("//h2/span[4]/text()").extract_first()
        
        item_loader.add_value("address", city+" "+district)
        item_loader.add_value("city", city)
        
        square_meters=response.xpath("//dl/div/dt[span[.='Metrekare']]//following-sibling::dd/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split())
            
        rent=response.xpath("//div[@class='title']/h3/span/text()").extract_first()
        if rent:
            rent = rent.split("TL")[0].strip().replace(".","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency","TRY")
        
        utilities=response.xpath("//dl/div/dt[span[.='Aidat']]//following-sibling::dd/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("TL")[0].strip())
        deposit=response.xpath("//dl/div/dt[span[.='Depozito']]//following-sibling::dd/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("TL")[0].strip())
        
        floor=response.xpath("//dl/div/dt[span[.='Bulunduğu Kat']]//following-sibling::dd/text()[not(contains(.,'Giriş')) and not(contains(.,'Bahçe Katı'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split())
            
        desc="".join(response.xpath("//span[@id='content_ctlRealtyDescription1_lblRealtyDescription']//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip().replace("_","").replace("\u25c6","").replace("\u0130","").replace("\u300a",""))
            item_loader.add_value("description", desc)
        
        
        balcony=response.xpath("//div[@class='c']/ul/li/span[.='Balkon']/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator=response.xpath("//div[@class='c']/ul/li/span[.='Asansör']/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished=response.xpath("//div[@class='c']/ul/li/span[.='Mobilyalı']/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        
        wash=response.xpath("//div[@class='c']/ul/li/span[.='Beyaz Eşyalı']/text()").extract_first()
        if wash:
            item_loader.add_value("washing_machine", True)
            item_loader.add_value("dishwasher", True)
            
        parking=response.xpath("//div[@class='c']/ul/li/span[contains(.,'Otopark')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        swimming_pool=response.xpath("//div[@class='c']/ul/li/span[.='Yüzme Havuzu']/text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        terrace=response.xpath("//div[@class='c']/ul/li/span[.='Teras']/text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        
        
        name=response.xpath("//div[@class='c']/span[@class='contact-name']/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
        else:
            name = response.xpath("//div[@class='user-info']/b[1]/text()").getall()
            item_loader.add_value("landlord_name", name[1])
        
        phone=response.xpath("//div[@class='c']/ul/li[2]/span[2]/text()").extract_first()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        else:
            phone = response.xpath("//div[@class='user-info']/span[2]/text()").getall()
            item_loader.add_value("landlord_phone", phone[1])

        images=[x for x in response.xpath("//div[@class='photo-gallery']/div/ul/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        lat_lng = response.xpath("//script/text()[contains(.,'ymaps.Placemark(')]").get()
        if lat_lng:             
            lat_lng = lat_lng.split("ymaps.Placemark([")[1].split("]")[0]
            item_loader.add_value("latitude",lat_lng.split(",")[0].strip())
            item_loader.add_value("longitude",lat_lng.split(",")[1].strip())
        yield item_loader.load_item()