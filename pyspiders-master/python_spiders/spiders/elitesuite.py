import scrapy
from scrapy.http.request import form
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import re
class ElitesuiteSpider(scrapy.Spider):
    name = 'elitesuite'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['elitesuite.ca']
    start_urls = ['https://elitesuite.ca/rentals/current']

    def parse(self, response):
        links = []
        items = response.css("a.RentalListItem")
        for item in items:
            if not item.css("div.RentalRented"):
                links.append(response.urljoin(item.css(".RentalListItem::attr(href)").get()))
        for link in links:
            yield scrapy.Request(url = link, callback=self.parse_page)
    
    
    def parse_page(self, response):
        full_images = []
        
        
        address     = response.css(".mr1.FlexGrow h2::text").get()
        room_count  = int(response.css(".mr1.FlexGrow div.LightBlue.FlexWrap div.AddressItem").re("[0-9]")[0])
        rent        = int(response.css(".mr1.FlexGrow div.LightBlue.FlexWrap div").re("\$[0-9]+,*[0-9]*")[0].replace("$",""))
        images      = response.css(".jcarousel ul li img::attr(src)").getall()
        utilities   = make_utils_val(rent, response.css(".contained.SectionPaddingTop.SectionPaddingBottom ::text").re("[0-9]+% of utilities"))
        description = remove_white_spaces(fix_desc(" ".join(response.css(".contained.SectionPaddingTop.SectionPaddingBottom p::text").getall())))
        
        # landlord_email, landlord_phone = response.css(".contained.SectionPaddingTop.SectionPaddingBottom p em::text").getall()
        
        city,property_type = make_city(response.css(".mr1.FlexGrow div div.AddressItem.Purple.mb1::text").get())
        pets_allowed,square_meters,bathroom_count,dishwasher,washing_machine,parking    = get_features(response.xpath('//div/h3[contains(text(), "Amenities")]/following-sibling::ul/li').css("::text").getall())
        
        
        title       = response.css(".mr1.FlexGrow h2::text").get()
        if "-" in title:
            line = title.split("-")
            title = remove_white_spaces(line[0])
            if not property_type:
                property_type = remove_white_spaces(line[1])
        
        if not property_type:
            property_type =  set_prop(property_type)
        for image in images:
            full_images.append(response.urljoin(image.replace("122x100","760x475")))
        
        
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("title"                  ,title)
        item.add_value("city"                   ,city)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,full_images)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"CAD")
        item.add_value("dishwasher"             ,dishwasher)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("utilities"              ,utilities)
        item.add_value("property_type"          ,get_prop(property_type))
        
        # item.add_value("landlord_email"         ,landlord_email)
        # item.add_value("landlord_name"          ,landlord_name)
        
        if room_count != 0:
            yield item.load_item()
        
        
def make_utils_val(rent, utils):
    if len(utils) > 0:
        utils = utils[0][:utils[0].find("%")]
        return round(rent * int(utils)/100)



def get_features(f):
    f = "-".join(f)
    pets = ''
    sq = re.findall("[0-9]+ [Ss]q",f)
    baths = re.findall("[0-9]+\WBathroom",f)
    dishwasher = ''
    washing = ''
    parking = ''
    
    if len(sq) > 0:
        sq = sq_feet_to_meters(int(sq[0].lower().replace("sq","").strip()))
    
    if len(baths) > 0:
        baths = int(float(baths[0].lower().replace("bathroom","").strip()))
    
    if "dishwasher" in f.lower():
        dishwasher = True
    
    if "washer" in f.lower():
        washing = True
    
    if "no pets" in f.lower():
        pets = False
    elif "pets" in f:
        pets = True
    if "garage" in f.lower() or "parking" in f.lower():
        parking = True
    
    return pets,sq,baths,dishwasher,washing,parking
    
def make_city(val):
    property_type = ''
    if "-" in val:
        property_type = val[:val.find("-")]
        property_type = re.sub("[0-9]+","",property_type).strip()
    val  = val.split(",")
    return val[-2], property_type

def set_prop(desc):

    types = ['basement', 'main floor','b','m','duplex']
    
    for t in types:
        if t in desc:
            return t

def get_prop(prop):
    if not prop:
        return ""
    types = ['basement', 'main floor','b','m','duplex']
    for t in types:
        if t in prop.lower() :
            return 'apartment'
    else:
        return prop

def fix_desc(desc):
    return re.sub("Elite Property.*","",desc)
