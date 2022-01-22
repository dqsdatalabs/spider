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
import dateparser

class MySpider(Spider):
    name = 'lc_immo_be'
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = '.'
    
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'text/html, */*; q=0.01',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.111 YaBrowser/21.2.1.107 Yowser/2.5 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://lc-immo.be/fr-BE/List/Index/7',
        'Accept-Language': 'tr,en;q=0.9',
        'Cookie': '_culture=fr-BE; ASP.NET_SessionId=1qz21fietbekuxx351tnoaf4; __RequestVerificationToken=xGEi9rZzO_akgbP9BJtkryY4nXP1tbJFxeD3TuytQS4IDR2luCTu10L7hNsYUWybhAcdQqab6IYwBZEf2dvAcZtS879D9zS02Z36z_WfOUc1; _culture=fr-BE'
    }
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://lc-immo.be/fr-BE/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Category&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined",
                "property_type" : "apartment"
            },
            {
                "url" : "http://lc-immo.be/fr-BE/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Category&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined",
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield Request(item["url"],
                            headers=self.headers,
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"]})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        for item in response.xpath("//div[@class='estate-list__item']/a/@href").getall():
            seen = True
            if "appartement" in response.urljoin(item): yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': 'apartment'})
            elif "maison" in response.urljoin(item): yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': 'house'})
            else: continue

        if page == 1 or seen:
            follow_url = response.url.replace("&CurrentPage=" + str(page - 1), "&CurrentPage=" + str(page))
            yield Request(follow_url,
                            headers=self.headers,
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
             
        item_loader.add_value("external_link", response.url) 
        property_type =  response.meta.get('property_type')
        prop_type =  response.xpath("//tr[th[.='Catégorie']]/td/text()[.='studio']").get()
        if prop_type:
            property_type = "studio"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", "Lc_Immo_PySpider_france")
        address = response.xpath("//h3[.='Adresse']/following-sibling::p[1]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)
            city_zip = title.split(" - ")[-2].strip()
            zipcode = city_zip.split(" ")[0]
            city = " ".join(city_zip.split(" ")[1:])
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)
            if not address:
                item_loader.add_value("address", city+" "+zipcode)

        available_date = response.xpath("//tr[th[.='Disponibilité']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        bathroom_count = response.xpath("//tr[th[.='Nombre de salle de bain']]/td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        room_count = response.xpath("//tr[th[.='Nombre de chambres']]/td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif property_type == "studio":
            item_loader.add_value("room_count", "1")
      
        description = "".join(response.xpath("//h2[.='Description']/following-sibling::p[1]//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())
        else:
            description = "".join(response.xpath("//h2[.='Description']/following-sibling::p//text()").getall())
            if description:
                item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//tr[th[.='Surface habitable']]/td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        item_loader.add_xpath("floor", "//tr[th[.='Étages (nombre)']]/td/text()")
        item_loader.add_xpath("external_id", "//tr[th[.='Référence']]/td/text()")
        item_loader.add_xpath("rent_string", "//h1//text()[contains(.,'€')]")
      
        images = [x for x in response.xpath("//div[@class='estate-detail-carousel__body']//div[@class='owl-estate-photo']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
            
        item_loader.add_value("landlord_name", "LAMBRECHT CONSULT SPRL")
        item_loader.add_value("landlord_phone", "+32 2 344 74 96")
        item_loader.add_value("landlord_email", "lc.immo@outlook.com")

        energy_label = response.xpath("//img[@class='estate-facts__peb']/@alt").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[-1].strip())
    
        parking = " ".join(response.xpath("//tr[th[.='Parking' or .='Garage']]/td/text()").getall())
        if parking:
            if "oui" in parking.lower():
                item_loader.add_value("parking", True)
            elif "non" in parking.lower():
                item_loader.add_value("parking", False)
        terrace = response.xpath("//tr[th[.='Terrasse']]/td/text()").get()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            elif "non" in terrace.lower():
                item_loader.add_value("terrace", False)
        elevator = response.xpath("//tr[th[.='Ascenseur']]/td/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            elif "non" in elevator.lower():
                item_loader.add_value("elevator", False)
        swimming_pool = response.xpath("//tr[th[.='Piscine']]/td/text()").get()
        if swimming_pool:
            if "oui" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
            elif "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
        furnished = response.xpath("//tr[th[.='Meublé']]/td/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            elif "non" in furnished.lower():
                item_loader.add_value("furnished", False)
        yield item_loader.load_item()
