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
from urllib.parse import urljoin 
import re 
import dateparser

class MySpider(Spider):
    name = 'ea2group_com'
    execution_type='testing'
    country='united_kingdom' 
    locale='en'
    external_source="Ea2group_PySpider_united_kingdom"
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "http://www.ea2group.com"
        }

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.ea2group.com/search.aspx?ListingType=6&areainformation=0%7c0%7c0%7c%7c71&areainformationname=UK&radius=0&igid=&imgid=&egid=&emgid=&category=1"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    def parse(self, response):
        page = response.meta.get("page", 1)
        for url in response.xpath("//section[@class='propertyHolder']//a[img[not(contains(@src,'Agreed'))]]/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
       
        if page==1:
            validation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").get()
            viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").get()
            generator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").get()
        else:
            if response.xpath("//text()[contains(.,'__EVENTVALIDATION')]").get():
                validation = response.xpath("//text()[contains(.,'__EVENTVALIDATION')]").get().split("__EVENTVALIDATION|")[1].split("|")[0]
                viewstate = response.xpath("//text()[contains(.,'__VIEWSTATE|')]").get().split("__VIEWSTATE|")[1].split("|")[0]
                generator = response.xpath("//text()[contains(.,'__VIEWSTATEGENERATOR|')]").get().split("__VIEWSTATEGENERATOR|")[1].split("|")[0]
    
        if response.xpath("//a[contains(@title,'Next to Page')]/text()").get():
            data = {
                "ctl00$ctl13": "ctl00$cntrlCenterRegion$ctl01$cntrlSearchResultsUpdatePanel|ctl00$cntrlCenterRegion$ctl01$cntrlPagingHeader",
                "ctl00_ctl13_TSM": ";;System.Web.Extensions, Version=3.5.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:16997a38-7253-4f67-80d9-0cbcc01b3057:ea597d4b:b25378d2;Telerik.Web.UI, Version=2012.2.724.35, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:c8b048d6-c2e5-465c-b4e7-22b9c95f45c5:16e4e7cd:f7645509:24ee1bba:f46195d3:874f8ea2:19620875:39040b5c:f85f9819:e330518b:2003d0b8:1e771326:c8618e41:aa288e2d;",
                "ctl00_cntrlItemTemplateActionTooltipManager_ClientState": "",
                "ctl00_Menu1_cntrlMenu_ClientState": "",
                "ctl00_cntrlCenterRegion_ctl01_cntrlSavedItems_cntrlTooltipManager_ClientState": "",
                "ctl00$cntrlCenterRegion$ctl01$ctl02$priceordering": "cntrlPriceDescending",
                "ctl00$cntrlCenterRegion$ctl01$ctl02$cntrlPrice": "Sort By Price Descending",
                "ctl00_cntrlCenterRegion_ctl01_ctl02_cntrlPrice_ClientState": "",
                "ctl00_cntrlCenterRegion_ctl01_cntrlMapSavedItems_cntrlTooltipManager_ClientState": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtMethod": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtClientCallback": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtRecordsPerPage": "10",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtPage": "1",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtPropertyReference": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtAdditionalReferences": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtMapReference": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtOutputType": "",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtAllowSavingControls": "1",
                "ctl00$cntrlCenterRegion$ctl01$cntrlMappingAjaxHelper$txtGeocodedDataOnly": "1",
                "ctl00$cntrlCenterRegion$ctl01$txtMappingSavedPropertyArgs": "",
                "__EVENTTARGET": "ctl00$cntrlCenterRegion$ctl01$cntrlPagingHeader",
                "__LASTFOCUS": "",
                "__VIEWSTATE": f"{viewstate}",
                "__EVENTARGUMENT": f"{page+1}",
                "__VIEWSTATEGENERATOR": f"{generator}",
                "__EVENTVALIDATION": f"{validation}",
                "__ASYNCPOST": "true"
                }
      
            url = "http://www.ea2group.com/search.aspx?ListingType=6&areainformation=0%7c0%7c0%7c%7c71&areainformationname=UK&radius=0&igid=&imgid=&egid=&emgid=&category=1"
            yield FormRequest(url, callback=self.parse, headers=self.headers,dont_filter=True,formdata=data, meta={"page": page+1})
     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[h2[.='Description']]/span//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: 
            desc = "".join(response.xpath("//td//li/text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else: 
                return
        item_loader.add_value("external_source", self.external_source)
        externalid=response.url
        if externalid:
            externalid=externalid.split("/")[-1].split(".")[0]
            item_loader.add_value("external_id",externalid)
        rent = response.xpath("//div//h3//text()").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.lower().split('£')[-1].split('per')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)*4)))    
                item_loader.add_value("currency", "GBP")    
            else:
                item_loader.add_value("rent_string",rent.split(" ")[0].replace(",",""))  
        address = response.xpath("//div//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
        
        desc = " ".join(response.xpath("//div[h2[.='Description']]/span//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_xpath("room_count", "//span[contains(@id,'lblBedrooms')]/text()")
        item_loader.add_xpath("bathroom_count", "//span[contains(@id,'_lblBathrooms')]/text()")
        item_loader.add_xpath("title", "//div//h1/text()")
        terrace = response.xpath("//div[@class='features']//li/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//div[@class='features']//li/text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//div[@class='features']//li/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
                
        images = [x for x in response.xpath("//div[@id='gallery-1']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//a[contains(@id,'cntrlFloorplan')]/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
      
        item_loader.add_value("landlord_email", "lettings@ea2group.com")
        item_loader.add_value("landlord_phone", "020 7702 3456")
        item_loader.add_value("landlord_name", "ea2 estate agency")
         
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None