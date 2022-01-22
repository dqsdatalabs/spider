# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime


class MySpider(Spider):
    name = "bathim_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.bathim.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&MaxPriceSlider=10000&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&MaxPage=0&EstateCount=5&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&CountrySearch=Undefined"
            },
            # {
            #     "url" : "https://www.bathim.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&MaxPriceSlider=10000&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&MaxPage=5&EstateCount=68&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&CountrySearch=Undefined",
            #     "property_type" : "apartment"
            # },
        ]# LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath(
            "//div[@class='estate-list__item']/a"
        ):
            property_type = item.xpath("./div[@class='estate-card__text-details']/text()").get()
            if "Flat" in property_type or "Studio" in property_type or "Duplex" in property_type:
                property_type = "apartment"
                follow_url = response.urljoin(item.xpath("./@href").get())
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "House" in property_type or "Penthouse" in property_type or "Villa" in property_type:
                property_type = "house"
                follow_url = response.urljoin(item.xpath("./@href").get())
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            
        next_page = response.xpath("//a[.='next page']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Bathim_PySpider_" + self.country + "_" + self.locale)
        title = "".join(
            response.xpath("//h1/span/text()[normalize-space()]").extract()
        ).strip().rstrip("-")
        item_loader.add_value(
            "title", title.replace("\r\n                    ", " ")
        )
        item_loader.add_value("external_link", response.url)
        
        

        rent = response.xpath("normalize-space(//h1//text()[contains(., '€')])").get()
        if rent:
            rent = rent.split(" ")[0]
            item_loader.add_value("rent", rent.replace(",", ""))
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath("external_id", "//tr[./th[.='Reference']]/td")

        square = response.xpath("//tr[./th[.='Habitable surface']]/td/text()").get()
        if square:
            square = square.split(" ")[0]
            item_loader.add_value("square_meters", square)
        
        studio = response.xpath("//tr[./th[.='Category']]/td/text()").get()
        if studio:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        room = response.xpath("//tr[./th[.='Number of bedrooms']]/td/text()").get()
        if room:
            if "0" in room:
                if "studio" in item_loader.get_collected_values("property_type") or 'studio' in studio.strip() :
                    item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room)

        bathroom = response.xpath("//tr[./th[.='Number of bathrooms']]/td/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)

        date = response.xpath(
            "//tr[./th[.='Availability']]/td/text()[contains(.,'/')]"
        ).extract_first()
        if date:
            item_loader.add_value(
                "available_date",
                datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d"),
            )
        desc = "".join(response.xpath("normalize-space(//div[h2[.='Description']]/p[1]/text())").getall())
        if desc:
            item_loader.add_value("description", desc)

        utilities = response.xpath("//tr[./th[.='Charges (€) (amount)']]/td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        elif "euros of provision of charges" in desc:
            utilities = desc.split("euros of provision of charges")[0].split("+")[-1].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
                
        item_loader.add_xpath("floor", "//tr[./th[.='Floors (number)']]/td/text()")
        
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='owl-estate-photo']/a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        address = response.xpath("//div[h2[.='Description']]/p[2]/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split("-")[1].strip()
            if zipcode.split(" ")[0].isdigit() and len(zipcode.split(" ")[0])==4:
                zipc = zipcode.split(" ")[0]
                city = zipcode.split(zipc)[1].strip()
                item_loader.add_value("zipcode", zipc)
                item_loader.add_value("city", city)
            else: 
                address = address.split("-")[-1].strip()
                zipcode = address.split(" ")[0]
                city = address.split(zipcode)[1].strip()
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", city)
        else: 
            address = title.split("-")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" ")[-1])
            item_loader.add_value("zipcode", address.split(" ")[0])

        washing = response.xpath("normalize-space(//div[h2[.='Description']]/p[1]/text()[contains(.,'washing')])").extract_first()
        if washing:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("normalize-space(//div[h2[.='Description']]/p[1]/text()[contains(.,'dishwasher')])").extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        terrace = response.xpath("//tr[th[.='Terrace']]/td//text()").get()
        if terrace:
            if "Yes" in terrace:
                item_loader.add_value("terrace", True)
            elif "No" in terrace:
                item_loader.add_value("terrace", False)
        
        furnished = response.xpath("//tr[th[.='Furnished']]/td//text()").get()
        if furnished:
            if "Yes" in furnished:
                item_loader.add_value("furnished", True)
            elif "No" in furnished:
                item_loader.add_value("furnished", False)

        terrace = response.xpath("//tr[th[.='Parking']]/td//text()").get()
        if terrace:
            if terrace == "Yes":
                item_loader.add_value("parking", True)
            elif terrace == "No":
                item_loader.add_value("parking", False)

        elevator = response.xpath("//tr[th[.='Elevator']]/td/text()").extract_first()
        if elevator:
            if "Yes" in elevator:
                item_loader.add_value("elevator", True)
            elif "No" in elevator:
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath("//tr[th[.='Balcony']]/td/text()").extract_first()
        if balcony:
            if "Yes" in balcony:
                item_loader.add_value("balcony", True)
            elif "No" in balcony:
                item_loader.add_value("balcony", False)
        else:
            balcony = response.xpath("normalize-space(//div[h2[.='Description']]/p[1]/text()[contains(.,'balcon')])").get()
            if balcony:
                item_loader.add_value("balcony", True)
        
        pool = response.xpath("//tr[@id='detail_322']/td/text()").extract_first()
        if pool:
            if "Yes" in pool:
                item_loader.add_value("swimming_pool", True)
            elif "No" in pool:
                item_loader.add_value("swimming_pool", False)
        else:
            pool = response.xpath("normalize-space(//div[h2[.='Description']]/p[1]/text()[contains(.,'wimming pool')])").get()
            if pool:
                item_loader.add_value("swimming_pool", True)
        
        energy_label = response.xpath(
            "//span/img[@class='estate-facts__peb']/@src"
        ).extract_first()
        if energy_label:
            item_loader.add_value(
                "energy_label", energy_label.split("-")[1].split(".")[0].upper()
            )

        item_loader.add_value("landlord_name", "BATHIM & CO.")
        item_loader.add_value("landlord_phone", "+32 (0)2 733 00 00")
        
        item_loader.add_value("landlord_email", "lth@bathim.be")

        
        yield item_loader.load_item()


def split_address(address, get):
    if "-" in address:
        temp = address.split(" - ")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1].strip()

        if get == "zip":
            return zip_code
        else:
            return city
