# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import math
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = "hendrix_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.hendrix.be/fr-BE/List/PartialListEstate/7?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateRef=&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=&MaxPrice=&MaxPriceSlider=&Rooms=&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&SelectedCities=&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&SelectedRegions=&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&GroundMinArea=&GroundMaxArea=&CurrentPage=0&MaxPage=5&EstateCount=66&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapMarker%5D&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=1543&isMobileDevice=False&SelectedCountries=&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)
    
    # 1. FOLLOWING
    def parse(self, response):
        # url="https://www.hendrix.be/fr/bien/a-vendre/appartement/1435-mont-saint-guibert/4156105"
        # yield Request(url, callback=self.populate_item)

        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath("//div[contains(@class,'estate-thumb-container')]/a[@class='estate-thumb']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            prop = item.xpath("./span[@class='estate-thumb-description']/h3/span[2]/text()").get()
           
            
            if "a-vendre" in follow_url:
                pass
            else:
                if "appartement" in prop:
                    prop = "apartment"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop})
                elif "maison" in prop:
                    prop = "house"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop})
                
                seen = True

        if page == 1 or seen:
            url = f"https://www.hendrix.be/fr-BE/List/PartialListEstate/7?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateRef=&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=&MaxPrice=&MaxPriceSlider=&Rooms=&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&SelectedCities=&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&SelectedRegions=&Regions=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&NewProjects=False&GroundMinArea=&GroundMaxArea=&CurrentPage={page}&MaxPage=5&EstateCount=66&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapMarker%5D&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=1543&isMobileDevice=False&SelectedCountries=&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined"
            yield Request(url, callback=self.parse, meta={"page": page + 1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)
                
        item_loader.add_value("external_source", "Hendrix_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_xpath(
            "title", "normalize-space(//div[@id='site-main']//h1/text())"
        )
     
        category = response.xpath("//tr[./th[.='Catégorie']]/td/text()").get()
        if "studio" in category.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        desc = "".join(response.xpath("//div[@id='detail_description']//text()").extract())
        if desc:
            desc = desc.split("Description")[1].strip()
            item_loader.add_value(
                "description", desc.strip()
            )

        rent = response.xpath("//span[@class='estate-text-emphasis']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].strip())
            item_loader.add_value("currency", "EUR")

        item_loader.add_xpath("external_id", "//tr[./th[.='Référence']]/td/text()")
        item_loader.add_value("external_link", response.url)

        square = response.xpath("//tr[./th[.='Surface habitable']]/td/text()").get()
        if square:
            s_meters =  square.split("m²")[0]
            meters = math.ceil(float(s_meters))
            item_loader.add_value("square_meters",str(meters))
                
        
        room = response.xpath("//tr[./th[.='Nombre de chambres']]/td/text()").get()
        if room:
            if "0" not in room:
                item_loader.add_value("room_count", room)
            else:
                prop_studio = "".join(response.xpath("//div[@class='estate-feature']/p/text()").extract())
                if prop_studio:
                    if "studio" in prop_studio.lower() and "0" in room:
                        item_loader.add_value("room_count", "1")

        
        available_date = response.xpath("//tr[th[.='Disponibilité']]/td/text()").get()
        if available_date:
            if "immédiatement" in available_date:
                available_date = datetime.now()
                date2 = available_date.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_xpath(
            "utilities", "//tr[./th[contains(.,'Charges (€)')]]/td[@class='value']/text()"
        )
        item_loader.add_xpath("floor", "//tr[./th[contains(.,'Étage') or contains(.,'Étages')]]/td/text()")

        deposit = response.xpath("//tr[./th[contains(.,'Garantie location')]]/td[@class='value']/text()").extract_first()
        if deposit:
            price = "".join(item_loader.get_collected_values("rent"))
            item_loader.add_value("deposit", int(price.replace(".",""))*int(deposit))
        
        # lat = " ".join(
        #     response.xpath(
        #         "//div[@id='content-maps']/script[contains(text(),'var latlng = ')]/text()"
        #     ).extract()
        # )

        # item_loader.add_value(
        #     "latitude", lat.split("LatLng(")[1].split(");")[0].split(",")[0].strip()
        # )
        # item_loader.add_value(
        #     "longitude",
        #     lat.split("LatLng(")[1].split(");")[0].split(",")[1].strip(),
        # )
        item_loader.add_xpath(
            "floor_plan_images",
            "//div[@class='estate-files']/ul/li/a/@href",
        )

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='col-md-8']/div/ul/li/a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        pets_allowed = response.xpath("//tr/th[contains(.,'Animaux')]/following-sibling::td/text()").get()
        if pets_allowed and "Oui" in pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        terrace = response.xpath("//tr[./th[.='Terrasse']]/td/text()").get()
        if terrace:
            if terrace == "Oui":
                item_loader.add_value("terrace", True)
            elif terrace == "Non":
                item_loader.add_value("terrace", False)

        furnished = response.xpath(
            "//tr[./th[.='Furnished']]/td/text() | //tr[./th[.='Meublé']]/td/text() "
        ).get()
        if furnished:
            if "Oui" in  furnished:
                item_loader.add_value("furnished", True)
            elif "Non" in furnished:
                item_loader.add_value("furnished", False)

        parking = response.xpath(
            "//tr[./th[.='Parking']]/td/text()"
        ).get()
        garage = response.xpath(
            "//tr[./th[.='Garage']]/td/text()"
        ).get()
        if parking or garage:
            if "Oui" in parking or "Oui" in garage:
                item_loader.add_value("parking", True)
            elif "Non" in parking or "Non" in garage:
                item_loader.add_value("parking", False)
        # else:
        #     parking = response.xpath("//div[@id='detail_description']/div/p[not(self::p[@class='ptm'])]//text()[contains(.,'parking') or contains(.,'garage')]").get()
        #     if parking:
        #         item_loader.add_value("parking", True)

        elevator = response.xpath(
            "//tr[./th[.='Ascenseur']]/td/text()"
        ).get()
        if elevator:
            if "Oui" in elevator:
                item_loader.add_value("elevator", True)
            elif "Non" in elevator:
                item_loader.add_value("elevator", False)

        swimming_pool = response.xpath(
            "//tr[./th[.='Piscine']]/td/text()"
        ).get()
        if swimming_pool:
            if swimming_pool == "Oui":
                item_loader.add_value("swimming_pool", True)
            elif swimming_pool == "Non":
                item_loader.add_value("swimming_pool", False)
        else:
            swimming_pool = response.xpath(
            "//div[@id='detail_description']/div/p[not(self::p[@class='ptm'])]//text()[contains(.,'piscine') or contains(.,'Piscine')]").get()
            if swimming_pool:
                item_loader.add_value("swimming_pool", True)
        
        phone = response.xpath(
            '//div[@class="col-md-3 col-xs-12 xs-tac tar"]/p[1]/a/@href'
        ).get()
        if phone:
            item_loader.add_value(
                "landlord_phone", phone.replace("tel:", "")
            )

        energy = response.xpath(
            "//div[@class='estate-feature']/img/@alt"
        ).get()
        if energy:
            item_loader.add_value("energy_label", energy.split(":")[1])

        address = response.xpath(
            "normalize-space(//div[@class='col-md-12 page-title']/h1/text())"
        ).get()
        if address:
            if "à louer" in address:
                address = (
                    address.split("à louer - ")[1].split(" ")[0]
                    + " "
                    + address.split("à louer - ")[1].split(" ")[1]
                )
            if "à vendre" in address:
                address = (
                    address.split("à vendre - ")[1].split(" ")[0]
                    + " "
                    + address.split("à vendre - ")[1].split(" ")[1]
                )
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        email = response.xpath(
            '//div[@class="division estate-contact"]/p/a/@href'
        ).get()
        if email:
            item_loader.add_value(
                "landlord_email", email.replace("mailto:", "")
            )
        item_loader.add_value("landlord_name", "immobilire Hendrix")
        item_loader.add_xpath(
            "city", "normalize-space(//p[@class='fz20']/text()[2])"
        )

        item_loader.add_xpath("bathroom_count", "//table[@class='estate-table']//tr[./th[contains(.,'Nombre de salle de bain')]]/td[@class='value']/text()")
        
        furnished = response.xpath("//div[@id='detail_description']/div/p[not(self::p[@class='ptm'])]//text()[contains(.,'meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)


        dishwasher = response.xpath("//div[@id='detail_description']/div/p[not(self::p[@class='ptm'])]//text()[contains(.,'lave-vaisselle') or contains(.,'Lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing = response.xpath("//div[@id='detail_description']/div/p[not(self::p[@class='ptm'])]//text()[contains(.,'Machine à laver') or contains(.,'machine à laver')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)


        yield item_loader.load_item()


def split_address(address, get):
    temp = address.split(" ")[0]
    zip_code = "".join(filter(lambda i: i.isdigit(), temp))
    city = address.split(" ")[1]

    if get == "zip":
        return zip_code
    else:
        return city
