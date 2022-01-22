# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
import time
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

class ImmoinvestSpider(scrapy.Spider):
    name = 'immoinvest'
    allowed_domains = ['immoinvest']
    start_urls = ['https://www.immoinvest.be/']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    custom_settings = {"HTTPCACHE_ENABLED": False}
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.immoinvest.be/fr-BE/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&MaxPriceSlider=10000&Rooms=0&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&CurrentPage=0&MaxPage=0&EstateCount=2&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined', 'property_type': 'house'},
            {'url': 'https://www.immoinvest.be/fr-BE/List/PartialListEstate?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=0&MaxPriceSlider=10000&Rooms=0&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SelectedRegion=0&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&CurrentPage=0&MaxPage=0&EstateCount=5&SoldEstates=False&RemoveSoldRentedOptionEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined', 'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse, meta={'property_type': url.get('property_type')})
    
    duplicate_list = []
    def parse(self, response, **kwargs):
        for link in response.xpath('//div[@class="estate-list__item"]//a[@class="estate-card"]'):
            url = response.urljoin(link.xpath('./@href').extract_first())
            price = ''.join(link.xpath('./div[@class="estate-card__text-details"]//text()').extract())
            if price:
                price = price.replace(".","")
            if 'appartement' in url or 'maison' in url: 
                if 'appartement' in url:
                    property_type = 'apartment'
                elif 'maison' in url:
                    property_type = 'house'
                if url not in self.duplicate_list:
                    self.duplicate_list.append(url)
                else:
                    continue
                
                yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'property_type': property_type, 'rent': price},
                    dont_filter=True
                )
    
    def get_property_details(self, response):
        external_link = response.url
        external_id = response.xpath('//th[contains(text(), "Référence")]/following-sibling::td/text()').extract_first('')
        prop_check = "".join(response.xpath("//div[@class='col-md-9']/p/text()").getall())
        if prop_check and "studio" in prop_check.lower():
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')
        city_zip = response.url.split('/')[-2]
        zipcode = city_zip.split('-')[0]
        city = city_zip.split('-')[1]
        address = 'Avenue de Waterloo, 45' + city + ' ' + zipcode
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first().replace('\t', '').replace('\n', '')
        images = []
        image_links = response.xpath('//div[@class="item"]//img')

        for image_link in image_links:
            image_url = image_link.xpath('./@src').extract_first()
            if image_url not in images:
                images.append(image_url)
        square_meters_text = response.xpath('//th[contains(text(), "Surface habitable")]/following-sibling::td/text()').extract_first('')
        details_text = ''.join(response.xpath('//meta[@property="og:description"]/@content').extract())
        if 'garage' in details_text.lower() or 'parking' in details_text.lower():
            parking = True
        else:
            parking = ''
        terrace_text = response.xpath('//th[contains(text(), "Terrasse")]/following-sibling::td/text()').extract_first('')
        if 'Oui' in terrace_text:
            terrace = True
        else:
            terrace = ''
        if square_meters_text:
            item_loader = ListingLoader(response=response)

            bathroom_count = response.xpath("//th[contains(.,'salle de bain')]/following-sibling::td/text()").get()
            if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('title', title)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            rent = response.meta.get('rent')
            if rent:
                rent = rent.split(" €")[0].split()[-1]
                item_loader.add_value('rent', rent)
            description = response.xpath("//meta[@property='og:description']/@content").get()
            item_loader.add_value('description', description)
            item_loader.add_xpath('square_meters', '//th[contains(text(), "habitable")]/following-sibling::td/text()')
            item_loader.add_xpath('utilities', '//th[contains(text(), "Charges (€)")]/following-sibling::td/text()')
            item_loader.add_xpath('floor', '//th[contains(text(), "Étages")]/following-sibling::td/text()')
            item_loader.add_value('images', images)
            deposit = response.xpath("//tr[@id='detail_573']/td/text()").get()
            if deposit:
                item_loader.add_value("deposit",int(deposit)*int(rent))
            elif "2 mois de caution" in description:
                item_loader.add_value("deposit",int(rent)*2)
            if terrace:
                item_loader.add_value('terrace', True)
            if parking:
                item_loader.add_value('parking', True)
            item_loader.add_xpath('room_count', '//th[contains(text(), "chambres")]/following-sibling::td/text()')
            item_loader.add_value('landlord_name', 'SA Immo Invest & Cie')
            item_loader.add_value('landlord_email', 'info@immoinvest.be')
            item_loader.add_value('landlord_phone', '071 30 67 90')
            item_loader.add_value('external_source', 'Immoinvest_PySpider_france_fr')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            item_loader.add_value("currency","EUR")
            yield item_loader.load_item()
