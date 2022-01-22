# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import re
from datetime import datetime


class MySpider(Spider):
    name = "eurohouse_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.eurohouse.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&MaxPage=19&EstateCount=238&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined",
                "type" : 1
            },
            {
                "url" : "http://www.eurohouse.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage=0&MaxPage=0&EstateCount=8&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined",
                "type" : 2
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={"type": url.get("type")})
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath(
            "//div[@class='estate-list__item']/a"
        ):
            follow_url = response.urljoin(item.xpath("./@href").get())
            prop_type = item.xpath("./div[@class='estate-card__text-details']/text()").get()
            if "Flat" in prop_type:
                prop_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
            elif "House" in prop_type:
                prop_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
            seen = True

        if page == 1 or seen:
            url = ""
            if response.meta.get("type") == 1:
                url = f"http://www.eurohouse.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage={page}&MaxPage=19&EstateCount=238&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined"
            elif response.meta.get("type") == 2:
                url = f"http://www.eurohouse.be/en/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&Rooms=0&ListID=21&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&CurrentPage={page}&MaxPage=0&EstateCount=8&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined"

            yield Request(url, callback=self.parse, meta={"page": page + 1, 'property_type': response.meta.get('property_type'), "type": response.meta.get("type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Eurohouse_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = "".join(
            response.xpath(
                "//div[@class='section-intro estate-detail-intro']/h1//text()"
            ).extract()
        )
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        
        title_first = title.split('-')[0].strip().lower().replace('\xa0', '')
        property_type = ''
        if title_first == 'studio':
            property_type = 'studio'
        elif title_first == 'flat' or title_first == 'ground floor' or title_first == 'exceptional apartment':
            property_type = 'apartment'
        elif title_first == 'duplex' or title_first == 'penthouse' or title_first == 'house':
            property_type = 'house'
        else:
            return
        item_loader.add_value("property_type", property_type)
        
        description = " ".join(response.xpath("//text()[preceding::h2[contains(.,'Description')] and following::h2[contains(.,'General')]]").getall()).strip()
        if description:
            item_loader.add_value("description", description)
            if "elevator" in description.lower():
                item_loader.add_value("elevator", True)
            if "washing machine" in description.lower() or "washer" in description.lower():
                item_loader.add_value("washing_machine", True)      
            if "balcony" in description.lower():
                item_loader.add_value("balcony", True)

            
        price = response.xpath("//h1//text()[contains(., '€')]").extract_first().strip()
        if price:
            item_loader.add_value("rent_string", price.replace(",", ""))

        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@class='item']//a/@href").extract()
        ]
        item_loader.add_value("images", images)

        item_loader.add_xpath("external_id", "//tr[th[.='Reference']]/td")
        square = response.xpath("//tr[th[.='Habitable surface']]/td/text()").extract_first()
        square_meters=response.xpath("//tr[th[contains(.,'Living (surface)')]]/td/text()").get()
        sq=response.xpath("//tr[th[contains(.,'Room')]]/td/text()").getall()
        total=0
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        elif sq:
            for i in range(0,len(sq)):
                square_meter=sq[i].split("m²")[0].strip()
                if "." in square_meter:
                    square_meter=square_meter.split(".")[0]
                total=total+int(square_meter)
            if total:
                item_loader.add_value("square_meters", str(total))
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0])  
        
        room = response.xpath("//tr[th[contains(.,'bedroom')]]/td//text()[.!='0']").get()
        if room:
            item_loader.add_value("room_count", room)
        elif "studio" in title.lower():
            item_loader.add_value("room_count", "1")
        else:
            if "studio" in item_loader.get_collected_values("property_type") and "0" in room:
                item_loader.add_value("room_count", "1")
        
        bathroom=response.xpath("//tr[th[contains(.,'bathroom')]]/td//text()[.!='0']").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)

        date = response.xpath(
            "//tr[th[.='Availability']]/td/text()[contains(.,'/')]"
        ).extract_first()
        if date:
            item_loader.add_value(
                "available_date",
                datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d"),
            )

        item_loader.add_xpath(
            "utilities", "//tr[th[.='Charges (€) (amount)']]/td"
        )
        
        floor=response.xpath("//tr[th[.='Floor']]/td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].strip())
        

        terrace = response.xpath("//tr[th[.='Terrace']]/td/text()").get()
        if terrace:
            if "Yes" in terrace:
                item_loader.add_value("terrace", True)
            elif "No" in terrace:
                item_loader.add_value("terrace", False)

        terrace = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if terrace:
            if "Yes" in terrace:
                item_loader.add_value("furnished", True)
            elif "No" in terrace:
                item_loader.add_value("furnished", False)

        parking = response.xpath("//th[contains(.,'Parking')]/following-sibling::td/text()").get()
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value("parking", True)
            elif "no" in parking.lower():
                item_loader.add_value("parking", False)

        terrace = response.xpath("//tr[th[.='Elevator']]/td/text()").get()
        if terrace:
            if "Yes" in terrace:
                item_loader.add_value("elevator", True)
            elif "No" in terrace:
                item_loader.add_value("elevator", False)

        item_loader.add_value("landlord_phone", "+32 2 672.05.55")
        item_loader.add_value("landlord_name", "Euro House")
        item_loader.add_value("landlord_email", "info@eurohouse.be")

        address = response.xpath(
            "//span[@class='estate-detail-intro__block-text'][2]/text()"
        ).extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        energy_label = response.xpath("//span/img/@src[contains(.,'peb')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("peb-")[1].split(".")[0].upper())
        else:
            energy_label = response.xpath("//th[contains(.,'Energy certificate (energy consumption')]/following-sibling::td/text()").get()
            if energy_label:
                energy_label = int(float(energy_label.strip()))
                if energy_label <= 120:
                    item_loader.add_value("energy_label", 'A')
                elif energy_label > 120 and energy_label <= 140:
                    item_loader.add_value("energy_label", 'B')
                elif energy_label > 140 and energy_label <= 155:
                    item_loader.add_value("energy_label", 'C')
                elif energy_label > 155 and energy_label <= 170:
                    item_loader.add_value("energy_label", 'D')
                elif energy_label > 170 and energy_label <= 190:
                    item_loader.add_value("energy_label", 'E')
                elif energy_label > 190 and energy_label <= 225:
                    item_loader.add_value("energy_label", 'F')
                elif energy_label > 225:
                    item_loader.add_value("energy_label", 'G')
            
        yield item_loader.load_item()


def split_address(address, get):
    temp = address.split(" ")[-2]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city
